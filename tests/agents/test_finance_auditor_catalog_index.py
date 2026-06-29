"""Testes do RAG sobre o catálogo (datasets/tabelas/colunas por significado).

Cobre:
- `catalog_index.py`: similaridade de cosseno, resumo textual de tabela,
  detecção de TTL expirado, `reindex_catalog` e `search_catalog` (com
  embeddings/BigQuery/DB mockados — sem chamadas reais).
- `cap_catalog_search` (capabilities.py): filtragem por RBAC.
- `cap_text_to_sql`: novo caminho totalmente autônomo (sem `table_refs` nem
  `dataset_ref`) que usa a busca semântica no catálogo em vez de exigir o
  dataset já conhecido.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# _cosine_similarity
# ---------------------------------------------------------------------------

class TestCosineSimilarity:
    def test_vetores_identicos(self):
        from src.agents.finance_auditor.catalog_index import _cosine_similarity

        assert _cosine_similarity([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)

    def test_vetores_ortogonais(self):
        from src.agents.finance_auditor.catalog_index import _cosine_similarity

        assert _cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)

    def test_vetor_zero_nao_quebra(self):
        from src.agents.finance_auditor.catalog_index import _cosine_similarity

        assert _cosine_similarity([0.0, 0.0], [1.0, 1.0]) == 0.0

    def test_tamanhos_diferentes_retorna_zero(self):
        from src.agents.finance_auditor.catalog_index import _cosine_similarity

        assert _cosine_similarity([1.0, 0.0], [1.0, 0.0, 0.0]) == 0.0


# ---------------------------------------------------------------------------
# _table_summary / _is_stale
# ---------------------------------------------------------------------------

class TestTableSummary:
    def test_inclui_dataset_tabela_e_descricoes(self):
        from src.agents.finance_auditor.catalog_index import _table_summary

        table = {
            "table_id": "clientes",
            "columns": [
                {"name": "id_cliente", "description": "ID unico do cliente (PK)"},
                {"name": "nome", "description": ""},
            ],
        }
        summary = _table_summary("ecommerce_saude", table)
        assert "ecommerce_saude.clientes" in summary
        assert "id_cliente (ID unico do cliente (PK))" in summary
        assert "nome" in summary


class TestIsStale:
    def test_sem_data_e_stale(self):
        from src.agents.finance_auditor.catalog_index import _is_stale

        assert _is_stale(None, 24) is True

    def test_data_recente_nao_e_stale(self):
        from src.agents.finance_auditor.catalog_index import _is_stale

        now_iso = datetime.now(timezone.utc).isoformat()
        assert _is_stale(now_iso, 24) is False

    def test_data_antiga_e_stale(self):
        from src.agents.finance_auditor.catalog_index import _is_stale

        old_iso = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        assert _is_stale(old_iso, 24) is True


# ---------------------------------------------------------------------------
# reindex_catalog
# ---------------------------------------------------------------------------

class TestReindexCatalog:
    def test_nao_reindexa_dentro_do_ttl(self):
        from src.agents.finance_auditor import catalog_index

        fresh_iso = datetime.now(timezone.utc).isoformat()
        with patch.object(catalog_index, "get_catalog_oldest_update", return_value=fresh_iso):
            result = catalog_index.reindex_catalog("proj", force=False)

        assert result["reindexed"] is False

    def test_reindexa_e_persiste_embeddings(self):
        from src.agents.finance_auditor import catalog_index

        fake_datasets = [{"dataset_id": "ecommerce_saude", "labels": {}}]
        fake_schema = {
            "tables": [
                {
                    "table_id": "clientes",
                    "full_name": "p.ecommerce_saude.clientes",
                    "columns": [{"name": "id_cliente", "description": "PK"}],
                },
            ]
        }
        fake_embeddings = MagicMock()
        fake_embeddings.embed_documents.return_value = [[0.1, 0.2, 0.3]]

        with patch.object(catalog_index, "get_catalog_oldest_update", return_value=None), \
             patch.object(catalog_index, "list_datasets_with_labels", return_value=fake_datasets), \
             patch.object(catalog_index, "get_dataset_tables_schema", return_value=fake_schema), \
             patch.object(catalog_index, "_get_embeddings", return_value=fake_embeddings), \
             patch.object(catalog_index, "upsert_catalog_entry") as mock_upsert:
            result = catalog_index.reindex_catalog("p", force=True)

        assert result["reindexed"] is True
        assert result["tables_indexed"] == 1
        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args.kwargs
        assert kwargs["dataset_id"] == "ecommerce_saude"
        assert kwargs["table_id"] == "clientes"
        assert json.loads(kwargs["embedding_json"]) == [0.1, 0.2, 0.3]

    def test_falha_ao_listar_datasets_nao_quebra(self):
        from src.agents.finance_auditor import catalog_index

        with patch.object(catalog_index, "get_catalog_oldest_update", return_value=None), \
             patch.object(catalog_index, "list_datasets_with_labels", side_effect=RuntimeError("boom")):
            result = catalog_index.reindex_catalog("p", force=True)

        assert result["reindexed"] is False


# ---------------------------------------------------------------------------
# sync_gold_metric_catalog
# ---------------------------------------------------------------------------

class TestSyncGoldMetricCatalog:
    def test_ignora_datasets_sem_a_tabela_gold(self):
        from src.agents.finance_auditor import catalog_index

        fake_datasets = [
            {"dataset_id": "dom_cobranca", "labels": {}},
            {"dataset_id": "dom_vendas", "labels": {}},
        ]
        with patch.object(catalog_index, "list_datasets_with_labels", return_value=fake_datasets), \
             patch.object(catalog_index, "list_table_ids", return_value=["BRONZE_X", "SILVER_X"]) as mock_list_tables, \
             patch.object(catalog_index, "execute_query_rows") as mock_query, \
             patch.object(catalog_index, "upsert_finance_metric") as mock_upsert:
            result = catalog_index.sync_gold_metric_catalog("proj")

        assert result == {"datasets_scanned": 2, "datasets_with_catalog": 0, "synced": 0, "errors": []}
        assert mock_list_tables.call_count == 2
        mock_query.assert_not_called()
        mock_upsert.assert_not_called()

    def test_sincroniza_so_o_dataset_que_tem_gold_metric_catalog(self):
        """Não pode haver hardcode de dataset — duas gerências (datasets
        diferentes), só uma tem GOLD_METRIC_CATALOG, só essa é sincronizada."""
        from src.agents.finance_auditor import catalog_index

        fake_datasets = [
            {"dataset_id": "dom_cobranca", "labels": {"gerencia": "cobranca"}},
            {"dataset_id": "dom_vendas", "labels": {"gerencia": "vendas"}},
        ]
        fake_rows = [
            {
                "METRIC_ID": "M001",
                "METRIC_NAME": "TAXA_INADIMPLENCIA",
                "DOMINIO": "COBRANCA",
                "DESCRICAO": "Percentual em atraso",
                "FORMULA_SQL": "SUM(VALOR_ABERTO)",
                "SQL_TEMPLATE": "SUM(CASE WHEN STATUS='ATRASADO' THEN VALOR_ABERTO END)",
                "OWNER": "GERENCIA_COBRANCA",
                "NIVEL": "ESTRATEGICO",
                "OFICIAL": True,
                "SOURCE_TABLE": "GOLD_FCT_CONTAS_RECEBER",
            },
        ]

        def fake_list_table_ids(project_id, dataset_id):
            return ["GOLD_METRIC_CATALOG"] if dataset_id == "dom_cobranca" else ["BRONZE_X"]

        with patch.object(catalog_index, "list_datasets_with_labels", return_value=fake_datasets), \
             patch.object(catalog_index, "list_table_ids", side_effect=fake_list_table_ids), \
             patch.object(catalog_index, "execute_query_rows", return_value=(fake_rows, 0)) as mock_query, \
             patch.object(catalog_index, "upsert_finance_metric") as mock_upsert:
            result = catalog_index.sync_gold_metric_catalog("proj")

        assert result["datasets_with_catalog"] == 1
        assert result["synced"] == 1
        mock_query.assert_called_once()
        assert "dom_cobranca.GOLD_METRIC_CATALOG" in mock_query.call_args[0][0]

        upsert_kwargs = mock_upsert.call_args.kwargs
        # Prefere METRIC_ID (estável) ao nome quando ambos existem.
        assert mock_upsert.call_args[0][0] == "proj.dom_cobranca.m001"
        assert upsert_kwargs["domain"] == "cobranca"
        assert upsert_kwargs["is_official"] is True
        assert upsert_kwargs["source_table"] == "proj.dom_cobranca.GOLD_FCT_CONTAS_RECEBER"

    def test_falha_ao_listar_datasets_nao_quebra(self):
        from src.agents.finance_auditor import catalog_index

        with patch.object(catalog_index, "list_datasets_with_labels", side_effect=RuntimeError("boom")):
            result = catalog_index.sync_gold_metric_catalog("proj")

        assert result["synced"] == 0
        assert result["errors"]

    def test_falha_pontual_de_um_dataset_nao_aborta_os_demais(self):
        from src.agents.finance_auditor import catalog_index

        fake_datasets = [
            {"dataset_id": "dom_a", "labels": {}},
            {"dataset_id": "dom_b", "labels": {}},
        ]

        def fake_list_table_ids(project_id, dataset_id):
            if dataset_id == "dom_a":
                raise RuntimeError("sem permissao")
            return ["GOLD_METRIC_CATALOG"]

        with patch.object(catalog_index, "list_datasets_with_labels", return_value=fake_datasets), \
             patch.object(catalog_index, "list_table_ids", side_effect=fake_list_table_ids), \
             patch.object(catalog_index, "execute_query_rows", return_value=([], 0)), \
             patch.object(catalog_index, "upsert_finance_metric"):
            result = catalog_index.sync_gold_metric_catalog("proj")

        assert result["datasets_with_catalog"] == 1
        assert len(result["errors"]) == 1


# ---------------------------------------------------------------------------
# search_catalog
# ---------------------------------------------------------------------------

class TestSearchCatalog:
    def test_retorna_top_k_ordenado_por_score(self):
        from src.agents.finance_auditor import catalog_index

        entries = [
            {
                "dataset_id": "logistica_vendas",
                "table_id": "contas",
                "full_name": "p.logistica_vendas.contas",
                "text_summary": "...",
                "embedding_json": json.dumps([1.0, 0.0]),
            },
            {
                "dataset_id": "ecommerce_saude",
                "table_id": "clientes",
                "full_name": "p.ecommerce_saude.clientes",
                "text_summary": "...",
                "embedding_json": json.dumps([0.0, 1.0]),
            },
        ]
        fake_embeddings = MagicMock()
        fake_embeddings.embed_query.return_value = [1.0, 0.0]

        with patch.object(catalog_index, "reindex_catalog", return_value={"reindexed": False}), \
             patch.object(catalog_index, "list_catalog_entries", return_value=entries), \
             patch.object(catalog_index, "_get_embeddings", return_value=fake_embeddings):
            results = catalog_index.search_catalog("p", "contas a receber", top_k=5)

        assert results[0]["dataset_id"] == "logistica_vendas"
        assert results[0]["score"] == 1.0

    def test_query_vazia_retorna_lista_vazia(self):
        from src.agents.finance_auditor import catalog_index

        assert catalog_index.search_catalog("p", "") == []

    def test_sem_entradas_retorna_vazio(self):
        from src.agents.finance_auditor import catalog_index

        with patch.object(catalog_index, "reindex_catalog", return_value={"reindexed": False}), \
             patch.object(catalog_index, "list_catalog_entries", return_value=[]):
            assert catalog_index.search_catalog("p", "algo") == []


# ---------------------------------------------------------------------------
# cap_catalog_search (capabilities.py)
# ---------------------------------------------------------------------------

class TestCapCatalogSearch:
    def test_query_ausente_retorna_erro(self):
        from src.agents.finance_auditor.capabilities import cap_catalog_search

        out = cap_catalog_search({}, {"project_id": "p"})
        assert out["ok"] is False

    def test_filtra_por_rbac(self):
        from src.agents.finance_auditor import capabilities

        matches = [
            {"dataset_id": "ecommerce_saude", "table_id": "clientes", "full_name": "p.ecommerce_saude.clientes", "score": 0.9},
            {"dataset_id": "negado", "table_id": "x", "full_name": "p.negado.x", "score": 0.8},
        ]
        with patch.object(capabilities.catalog_index, "search_catalog", return_value=matches), \
             patch.object(capabilities.rbac, "check_dataset", side_effect=lambda user, ds: (ds != "negado", "")):
            out = capabilities.cap_catalog_search({"query": "clientes"}, {"project_id": "p"})

        assert out["ok"] is True
        refs = [m["table_ref"] for m in out["payload"]["matches"]]
        assert refs == ["p.ecommerce_saude.clientes"]


# ---------------------------------------------------------------------------
# cap_text_to_sql — caminho RAG totalmente autônomo
# ---------------------------------------------------------------------------

class TestTextToSqlCatalogRagPath:
    def test_sem_table_refs_e_dataset_ref_usa_catalog_search(self):
        from src.agents.finance_auditor import capabilities

        matches = [
            {"dataset_id": "logistica_vendas", "table_id": "contas", "full_name": "p.logistica_vendas.contas", "score": 0.9},
        ]
        sql_resp = MagicMock()
        sql_resp.sql = "SELECT * FROM `p.logistica_vendas.contas` LIMIT 10"
        sql_struct = MagicMock()
        sql_struct.invoke.return_value = sql_resp
        llm = MagicMock()
        llm.with_structured_output.return_value = sql_struct

        fake_dry = MagicMock(error=None, bytes_processed=10, estimated_cost_usd=0.0)

        with patch.object(capabilities.catalog_index, "search_catalog", return_value=matches), \
             patch.object(capabilities.rbac, "check_dataset", return_value=(True, "")), \
             patch.object(capabilities, "get_table_schema", return_value="schema"), \
             patch.object(capabilities, "dry_run_query", return_value=fake_dry), \
             patch.object(capabilities, "execute_query_rows", return_value=([{"x": 1}], 0)), \
             patch.object(capabilities, "get_runtime_config", return_value=str(5 * 1024 ** 3)):
            out = capabilities.cap_text_to_sql(
                {"natural_language": "contas a receber"},
                {"project_id": "p", "llm": llm},
            )

        assert out["ok"] is True
        assert out["payload"]["table_refs"] == ["p.logistica_vendas.contas"]
        assert "busca semântica" in out["payload"]["auto_picked_note"]

    def test_catalog_search_sem_resultado_retorna_erro(self):
        from src.agents.finance_auditor import capabilities

        with patch.object(capabilities.catalog_index, "search_catalog", return_value=[]):
            out = capabilities.cap_text_to_sql(
                {"natural_language": "algo bem obscuro"},
                {"project_id": "p", "llm": MagicMock()},
            )
        assert out["ok"] is False

    def test_explicit_table_refs_nao_aciona_catalog_search(self):
        from src.agents.finance_auditor import capabilities

        with patch.object(capabilities.catalog_index, "search_catalog") as mock_search:
            capabilities.cap_text_to_sql(
                {"natural_language": "q", "table_refs": ["ds.tbl"]},
                {"project_id": "p", "llm": MagicMock()},
            )
        mock_search.assert_not_called()
