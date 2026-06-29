"""Testes da Fase 3 do Finance Voice IA — governança e Semantic Layer.

Cobre componentes puros sem chamadas reais a BigQuery, LLM ou SQLite:
- PII Guard (regex, scrub, modos mask/block/off)
- RBAC (allow/deny/strict, slug, wildcard, admin bypass)
- Semantic Layer (search lexical, render_sql com placeholders)
- Audit (summarize_costs)
- Capabilities metric_lookup / metric_execute (com mocks de DB)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# PII Guard
# ---------------------------------------------------------------------------

class TestPIIGuard:
    def test_scan_detecta_cpf_email_cnpj(self):
        from src.shared.guardrails.pii_guard import scan

        text = "Cliente Jose, CPF 123.456.789-09, email a@b.com, CNPJ 12.345.678/0001-90"
        counts = scan(text)
        assert counts.get("cpf") == 1
        assert counts.get("email") == 1
        assert counts.get("cnpj") == 1

    def test_scrub_text_mascara_mantendo_4_finais(self):
        from src.shared.guardrails.pii_guard import scrub_text

        out, counts = scrub_text("CPF 123.456.789-09")
        assert counts.get("cpf") == 1
        assert "789-09" not in out and "789_09" not in out
        assert "***" in out and out.endswith("]")

    def test_apply_guard_off_passthrough(self):
        from src.shared.guardrails import pii_guard

        with patch.object(pii_guard, "_resolve_mode", return_value="off"):
            out = pii_guard.apply_guard("CPF 123.456.789-09", [{"type": "sql", "sql": "x"}])
        assert out["mode"] == "off"
        assert out["final_answer"] == "CPF 123.456.789-09"
        assert out["pii_counts"] == {}
        assert out["blocked"] is False

    def test_apply_guard_mask_scrub_artifacts(self):
        from src.shared.guardrails import pii_guard

        artifacts = [
            {"type": "table", "title": "x", "columns": ["email"],
             "rows": [{"email": "a@b.com"}]},
            {"type": "sql", "sql": "SELECT * WHERE cpf='111.222.333-44'"},
        ]
        with patch.object(pii_guard, "_resolve_mode", return_value="mask"):
            out = pii_guard.apply_guard("contato a@b.com", artifacts)
        assert out["mode"] == "mask"
        assert "a@b.com" not in out["final_answer"]
        assert out["artifacts"][0]["rows"][0]["email"].startswith("[email_REDACTED]")
        assert "111.222.333-44" not in out["artifacts"][1]["sql"]
        assert out["pii_counts"].get("email", 0) >= 1
        assert out["pii_counts"].get("cpf", 0) >= 1
        assert out["blocked"] is False

    def test_apply_guard_block_bloqueia_resposta_quando_ha_pii(self):
        from src.shared.guardrails import pii_guard

        with patch.object(pii_guard, "_resolve_mode", return_value="block"):
            out = pii_guard.apply_guard("CPF 111.222.333-44 do cliente", [])
        assert out["mode"] == "block"
        assert out["blocked"] is True
        assert out["pii_counts"].get("cpf") == 1
        assert "bloqueada" in out["final_answer"].lower()
        assert out["artifacts"] == []


# ---------------------------------------------------------------------------
# RBAC
# ---------------------------------------------------------------------------

class TestRBAC:
    def test_admin_sempre_passa(self):
        from src.shared.guardrails import rbac

        ok, _ = rbac.check_dataset({"is_admin": True}, "qualquer_dataset")
        assert ok is True
        ok, _ = rbac.check_metric({"is_admin": True}, "qualquer_metrica")
        assert ok is True

    def test_sem_acl_libera_quando_nao_strict(self):
        from src.shared.guardrails import rbac

        with patch.object(rbac, "_resolve_acl", return_value=None), \
             patch.object(rbac, "_strict_mode", return_value=False):
            ok, _ = rbac.check_dataset({"username": "u1"}, "x")
        assert ok is True

    def test_sem_acl_bloqueia_quando_strict(self):
        from src.shared.guardrails import rbac

        with patch.object(rbac, "_resolve_acl", return_value=None), \
             patch.object(rbac, "_strict_mode", return_value=True):
            ok, reason = rbac.check_dataset({"username": "u1"}, "x")
        assert ok is False
        assert "strict" in reason.lower()

    def test_allowlist_e_denylist(self):
        from src.shared.guardrails import rbac

        acl = {
            "allowed_datasets": ["ecommerce_saude", "logistica_*"],
            "allowed_metrics": [],
            "denied_datasets": ["financeiro"],
        }
        with patch.object(rbac, "_resolve_acl", return_value=acl):
            ok, _ = rbac.check_dataset({"username": "u1"}, "ecommerce_saude")
            assert ok is True
            ok, _ = rbac.check_dataset({"username": "u1"}, "logistica_vendas")
            assert ok is True  # wildcard prefix
            ok, reason = rbac.check_dataset({"username": "u1"}, "financeiro")
            assert ok is False and "negado" in reason.lower()
            ok, reason = rbac.check_dataset({"username": "u1"}, "marketing")
            assert ok is False and "allowlist" in reason.lower()

    def test_slug_normaliza_acentos(self):
        from src.shared.guardrails.rbac import _slug

        assert _slug("Logística Vendas") == "logistica_vendas"


# ---------------------------------------------------------------------------
# Semantic Layer
# ---------------------------------------------------------------------------

class TestSemanticLayer:
    def test_search_metrics_ordena_por_overlap(self):
        from src.agents.finance_auditor import semantic_layer

        fake = [
            {"key": "m1", "name": "Receita por mês",
             "description": "soma de pedidos por mes",
             "source_table": "x", "tags": "vendas"},
            {"key": "m2", "name": "Ticket médio",
             "description": "valor médio dos pedidos por categoria",
             "source_table": "x", "tags": ""},
            {"key": "m3", "name": "Churn",
             "description": "clientes que pararam de comprar",
             "source_table": "x", "tags": ""},
        ]
        with patch.object(semantic_layer, "list_finance_metrics", return_value=fake):
            out = semantic_layer.search_metrics("ticket medio pedidos", top_k=3)
        keys = [m["key"] for m in out]
        assert keys and keys[0] == "m2"  # melhor overlap

    def test_search_metrics_normaliza_plural_e_limita_top_k(self):
        from src.agents.finance_auditor import semantic_layer

        fake = [
            {"key": "pedidos_por_dia", "name": "Pedidos por dia",
             "description": "contagem de pedido diario",
             "source_table": "x", "tags": "vendas"},
            {"key": "clientes_ativos", "name": "Clientes ativos",
             "description": "base ativa", "source_table": "x", "tags": ""},
        ]
        with patch.object(semantic_layer, "list_finance_metrics", return_value=fake):
            out = semantic_layer.search_metrics("pedido diario", top_k=99)
        assert [m["key"] for m in out] == ["pedidos_por_dia"]

    def test_render_sql_substitui_placeholders(self):
        from src.agents.finance_auditor.semantic_layer import render_sql

        sql, used = render_sql(
            "SELECT * FROM t WHERE dt BETWEEN '{date_start}' AND '{date_end}' LIMIT {limit}",
            {"date_start": "2026-01-01", "date_end": "2026-01-31", "limit": 50},
        )
        assert "2026-01-01" in sql and "2026-01-31" in sql and "LIMIT 50" in sql
        assert used == {"date_start": "2026-01-01", "date_end": "2026-01-31", "limit": 50}

    def test_render_sql_aplica_defaults(self):
        from src.agents.finance_auditor.semantic_layer import render_sql

        sql, used = render_sql("SELECT '{date_start}'", {})
        assert "date_start" in used and used["date_start"]  # default aplicado

    def test_render_sql_normaliza_datas_limite_e_preserva_placeholder_desconhecido(self):
        from src.agents.finance_auditor.semantic_layer import render_sql

        sql, used = render_sql(
            "SELECT '{date_start}' AS ds, '{date_end}' AS de, {limit} AS lim, {custom} AS extra",
            {"date_start": "2026-03-10", "date_end": "2026-01-01", "limit": "5000"},
        )
        assert "2026-01-01" in sql and "2026-03-10" in sql
        assert "1000 AS lim" in sql
        assert "{custom}" in sql
        assert used == {"date_start": "2026-01-01", "date_end": "2026-03-10", "limit": 1000}

    def test_search_metrics_official_only_filtra_nao_oficiais(self):
        from src.agents.finance_auditor import semantic_layer

        fake = [
            {"key": "cobranca_oficial", "name": "Inadimplência oficial",
             "description": "metrica governada de cobranca", "source_table": "x",
             "tags": "cobranca", "domain": "cobranca", "is_official": True},
            {"key": "cobranca_ad_hoc", "name": "Inadimplência (rascunho)",
             "description": "metrica de cobranca ainda nao validada", "source_table": "x",
             "tags": "cobranca", "domain": "cobranca", "is_official": False},
        ]
        with patch.object(semantic_layer, "list_finance_metrics", return_value=fake):
            out = semantic_layer.search_metrics("cobranca", top_k=5, official_only=True)
        assert [m["key"] for m in out] == ["cobranca_oficial"]

    def test_pick_gold_metric_elege_principal_do_dominio(self):
        from src.agents.finance_auditor import semantic_layer

        fake = [
            {"key": "cobranca_oficial", "name": "Inadimplência oficial",
             "description": "metrica governada de cobranca", "source_table": "x",
             "tags": "cobranca", "domain": "cobranca", "is_official": True},
            {"key": "vendas_oficial", "name": "Receita oficial",
             "description": "metrica governada de vendas", "source_table": "x",
             "tags": "vendas", "domain": "vendas", "is_official": True},
        ]
        with patch.object(semantic_layer, "list_finance_metrics", return_value=fake):
            picked = semantic_layer.pick_gold_metric("cobranca")
        assert picked is not None
        assert picked["key"] == "cobranca_oficial"

    def test_pick_gold_metric_sem_match_retorna_none(self):
        from src.agents.finance_auditor import semantic_layer

        fake = [
            {"key": "vendas_oficial", "name": "Receita oficial",
             "description": "metrica governada de vendas", "source_table": "x",
             "tags": "vendas", "domain": "vendas", "is_official": True},
        ]
        with patch.object(semantic_layer, "list_finance_metrics", return_value=fake):
            assert semantic_layer.pick_gold_metric("logistica") is None

    def test_resolve_metric_normaliza_key_e_nome(self):
        from src.agents.finance_auditor import semantic_layer

        fake = [
            {"key": "ticket_medio", "name": "Ticket medio", "sql_template": "SELECT 1"},
        ]
        with patch.object(semantic_layer, "get_finance_metric", return_value=None), \
             patch.object(semantic_layer, "list_finance_metrics", return_value=fake):
            out = semantic_layer.resolve_metric("Tícket Médio")
        assert out == fake[0]


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------

class TestAudit:
    def test_summarize_costs(self):
        from src.shared.guardrails.audit import summarize_costs

        results = [
            {"payload": {"bytes_processed": 1024, "estimated_cost_usd": 0.001}},
            {"payload": {"bytes_processed": 2048, "estimated_cost_usd": 0.002}},
            {"payload": {"other": True}},
        ]
        out = summarize_costs(results)
        assert out["bytes_processed"] == 3072
        assert out["estimated_cost_usd"] == pytest.approx(0.003)


# ---------------------------------------------------------------------------
# Capability: metric_lookup
# ---------------------------------------------------------------------------

class TestCapMetricLookup:
    def test_busca_e_filtra_por_rbac(self):
        from src.agents.finance_auditor import capabilities

        fake_matches = [
            {"key": "vendas_diarias", "name": "Vendas diárias",
             "description": "x", "source_table": "y", "tags": "z"},
            {"key": "margem", "name": "Margem",
             "description": "x", "source_table": "y", "tags": "z"},
        ]
        with patch.object(capabilities.semantic_layer, "search_metrics", return_value=fake_matches), \
             patch.object(capabilities.rbac, "check_metric",
                          side_effect=lambda u, k: (k == "vendas_diarias", "")):
            out = capabilities.cap_metric_lookup(
                {"query": "vendas"}, {"user": {"username": "u1"}}
            )
        assert out["ok"] is True
        keys = [r["key"] for r in out["payload"]["matches"]]
        assert keys == ["vendas_diarias"]

    def test_official_only_e_repassado_e_aparece_no_payload(self):
        from src.agents.finance_auditor import capabilities

        fake_matches = [
            {"key": "cobranca_oficial", "name": "Inadimplência oficial",
             "description": "x", "source_table": "y", "tags": "cobranca",
             "domain": "cobranca", "is_official": True},
        ]
        with patch.object(
            capabilities.semantic_layer, "search_metrics", return_value=fake_matches
        ) as mock_search, patch.object(
            capabilities.rbac, "check_metric", return_value=(True, "")
        ):
            out = capabilities.cap_metric_lookup(
                {"query": "cobranca", "official_only": True}, {"user": {"username": "u1"}}
            )
        assert mock_search.call_args.kwargs["official_only"] is True
        assert out["payload"]["matches"][0]["is_official"] is True
        assert out["payload"]["matches"][0]["domain"] == "cobranca"


# ---------------------------------------------------------------------------
# Capability: metric_execute
# ---------------------------------------------------------------------------

class TestCapMetricExecute:
    def test_metrica_inexistente(self):
        from src.agents.finance_auditor import capabilities

        with patch.object(capabilities.rbac, "check_metric", return_value=(True, "")), \
             patch.object(capabilities.semantic_layer, "resolve_metric", return_value=None):
            out = capabilities.cap_metric_execute(
                {"key": "foo"}, {"project_id": "p"}
            )
        assert out["ok"] is False
        assert "não encontrada" in out["error"]

    def test_bloqueado_por_rbac(self):
        from src.agents.finance_auditor import capabilities

        with patch.object(capabilities.rbac, "check_metric", return_value=(False, "negada")):
            out = capabilities.cap_metric_execute(
                {"key": "foo"}, {"project_id": "p"}
            )
        assert out["ok"] is False
        assert "RBAC" in out["error"]

    def test_fluxo_feliz_executa_sql_renderizado(self):
        from src.agents.finance_auditor import capabilities

        metric = {
            "key": "vendas",
            "name": "Vendas",
            "sql_template": "SELECT 1 AS n FROM `p.d.t` WHERE dt='{date_start}' LIMIT {limit}",
        }
        fake_dry = MagicMock(error=None, bytes_processed=512, estimated_cost_usd=0.0)

        with patch.object(capabilities.rbac, "check_metric", return_value=(True, "")), \
             patch.object(capabilities.semantic_layer, "resolve_metric", return_value=metric), \
             patch.object(capabilities.rbac, "check_dataset", return_value=(True, "")), \
             patch.object(capabilities, "dry_run_query", return_value=fake_dry), \
             patch.object(capabilities, "execute_query_rows", return_value=([{"n": 1}], 0)), \
             patch.object(capabilities, "get_runtime_config", return_value=str(5 * 1024 ** 3)):
            out = capabilities.cap_metric_execute(
                {"key": "vendas", "params": {"date_start": "2026-01-01", "limit": 10}},
                {"project_id": "p", "user": {}},
            )
        assert out["ok"] is True
        assert out["payload"]["metric_key"] == "vendas"
        assert out["payload"]["params_used"]["date_start"] == "2026-01-01"
        assert out["payload"]["rows"] == [{"n": 1}]

    def test_metrica_sem_sql_template_da_erro_claro(self):
        from src.agents.finance_auditor import capabilities

        metric = {"key": "vendas", "name": "Vendas", "sql_template": ""}
        with patch.object(capabilities.rbac, "check_metric", return_value=(True, "")), \
             patch.object(capabilities.semantic_layer, "resolve_metric", return_value=metric):
            out = capabilities.cap_metric_execute({"key": "vendas"}, {"project_id": "p"})
        assert out["ok"] is False
        assert "sql_template" in out["error"]

    def test_gold_metric_catalog_expressao_sem_source_table_da_erro(self):
        """Métrica veio do Gold Metric Catalog (expressão pura, sem SELECT)
        mas a linha original não tinha SOURCE_TABLE preenchido — não dá pra
        montar a query, e o erro precisa deixar isso claro (não travar)."""
        from src.agents.finance_auditor import capabilities

        metric = {"key": "k", "name": "Valor recuperado", "sql_template": "SUM(VALOR_PAGO)", "source_table": ""}
        with patch.object(capabilities.rbac, "check_metric", return_value=(True, "")), \
             patch.object(capabilities.semantic_layer, "resolve_metric", return_value=metric):
            out = capabilities.cap_metric_execute({"key": "k"}, {"project_id": "p"})
        assert out["ok"] is False
        assert "SOURCE_TABLE" in out["error"]

    def test_gold_metric_catalog_monta_query_com_coluna_de_data_detectada(self):
        """Expressão pura (sem SELECT) + SOURCE_TABLE: monta o SELECT sozinho,
        descobrindo a coluna de data via schema real — sem nome de coluna
        hardcoded, já que cada gerência/tabela fato pode ter um nome diferente."""
        from src.agents.finance_auditor import capabilities

        metric = {
            "key": "p.d.taxa_inadimplencia",
            "name": "Taxa de inadimplência",
            "sql_template": "SAFE_DIVIDE(SUM(VALOR_ABERTO), SUM(VALOR_ORIGINAL))",
            "source_table": "p.d.GOLD_FCT_CONTAS_RECEBER",
        }
        fake_dry = MagicMock(error=None, bytes_processed=512, estimated_cost_usd=0.0)
        fake_columns = {"DATA_REFERENCIA": "DATE", "DT_PROCESSAMENTO": "TIMESTAMP", "VALOR_ABERTO": "NUMERIC"}

        with patch.object(capabilities.rbac, "check_metric", return_value=(True, "")), \
             patch.object(capabilities.semantic_layer, "resolve_metric", return_value=metric), \
             patch.object(capabilities.rbac, "check_dataset", return_value=(True, "")), \
             patch.object(capabilities, "get_table_column_types", return_value=fake_columns) as mock_cols, \
             patch.object(capabilities, "dry_run_query", return_value=fake_dry), \
             patch.object(capabilities, "execute_query_rows", return_value=([{"data_referencia": "2026-01-01", "valor": 0.05}], 0)), \
             patch.object(capabilities, "get_runtime_config", return_value=str(5 * 1024 ** 3)):
            out = capabilities.cap_metric_execute(
                {"key": "p.d.taxa_inadimplencia", "params": {"date_start": "2026-01-01", "date_end": "2026-01-31"}},
                {"project_id": "p", "user": {}},
            )

        assert out["ok"] is True
        mock_cols.assert_called_once_with("p.d.GOLD_FCT_CONTAS_RECEBER", "p")
        sql = out["payload"]["sql"]
        assert "DATA_REFERENCIA" in sql
        assert "SAFE_DIVIDE(SUM(VALOR_ABERTO), SUM(VALOR_ORIGINAL))" in sql
        assert "DT_PROCESSAMENTO" not in sql  # nunca escolhe coluna de ETL/carga como data de referência
        assert out["payload"]["rows"] == [{"data_referencia": "2026-01-01", "valor": 0.05}]

    def test_gold_metric_catalog_sem_coluna_de_data_monta_query_agregada(self):
        from src.agents.finance_auditor import capabilities

        metric = {
            "key": "p.d.metrica",
            "name": "Métrica",
            "sql_template": "COUNT(DISTINCT ID_CLIENTE)",
            "source_table": "p.d.GOLD_DIM_CLIENTE",
        }
        fake_dry = MagicMock(error=None, bytes_processed=10, estimated_cost_usd=0.0)

        with patch.object(capabilities.rbac, "check_metric", return_value=(True, "")), \
             patch.object(capabilities.semantic_layer, "resolve_metric", return_value=metric), \
             patch.object(capabilities.rbac, "check_dataset", return_value=(True, "")), \
             patch.object(capabilities, "get_table_column_types", return_value={"ID_CLIENTE": "STRING"}), \
             patch.object(capabilities, "dry_run_query", return_value=fake_dry), \
             patch.object(capabilities, "execute_query_rows", return_value=([{"valor": 42}], 0)), \
             patch.object(capabilities, "get_runtime_config", return_value=str(5 * 1024 ** 3)):
            out = capabilities.cap_metric_execute({"key": "p.d.metrica"}, {"project_id": "p", "user": {}})

        assert out["ok"] is True
        assert "WHERE" not in out["payload"]["sql"]
        assert "GROUP BY" not in out["payload"]["sql"]


class TestPickDateColumnAndBareExpression:
    def test_is_bare_sql_expression_detecta_expressao_sem_select(self):
        from src.agents.finance_auditor.capabilities import _is_bare_sql_expression

        assert _is_bare_sql_expression("SUM(VALOR_ABERTO)") is True
        assert _is_bare_sql_expression("SELECT 1 AS n FROM t") is False
        assert _is_bare_sql_expression("") is False

    def test_pick_date_column_prefere_data_referencia(self):
        from src.agents.finance_auditor.capabilities import _pick_date_column

        cols = {"DT_PROCESSAMENTO": "TIMESTAMP", "DATA_REFERENCIA": "DATE", "DT_VENCIMENTO": "DATE"}
        assert _pick_date_column(cols) == "DATA_REFERENCIA"

    def test_pick_date_column_ignora_colunas_de_etl(self):
        from src.agents.finance_auditor.capabilities import _pick_date_column

        cols = {"DT_PROCESSAMENTO": "TIMESTAMP", "DT_CADASTRO": "DATE", "DT_VENCIMENTO": "DATE"}
        assert _pick_date_column(cols) == "DT_VENCIMENTO"

    def test_pick_date_column_sem_coluna_de_data_retorna_none(self):
        from src.agents.finance_auditor.capabilities import _pick_date_column

        assert _pick_date_column({"VALOR": "NUMERIC", "ID_CLIENTE": "STRING"}) is None


# ---------------------------------------------------------------------------
# RBAC integrado: bq_query bloqueia dataset não-permitido
# ---------------------------------------------------------------------------

class TestRBACInBqQuery:
    def test_bq_query_bloqueia_dataset_nao_permitido(self):
        from src.agents.finance_auditor import capabilities

        with patch.object(capabilities.rbac, "check_dataset", return_value=(False, "negado")):
            out = capabilities.cap_bq_query(
                {"sql": "SELECT * FROM `p.financeiro.t`"},
                {"project_id": "p", "user": {"username": "u1"}},
            )
        assert out["ok"] is False
        assert "RBAC" in out["error"]


# ---------------------------------------------------------------------------
# Supervisor: nó audit grava em DB; nó guardrails_out aplica PII
# ---------------------------------------------------------------------------

class TestSupervisorNewNodes:
    def test_node_audit_persiste_e_devolve_id(self):
        from src.agents.finance_auditor import supervisor

        with patch.object(supervisor.audit_log, "record", return_value=42):
            out = supervisor.node_audit(
                {"user_id": "u1", "persona": "diretor",
                 "request_text": "x", "plan": [{"capability": "voc"}],
                 "tool_results": [{"ok": True}]}
            )
        assert out == {"audit_id": 42}

    def test_node_guardrails_out_aplica_pii_e_warning(self):
        from src.agents.finance_auditor import supervisor

        with patch.object(supervisor.pii_guard, "apply_guard", return_value={
            "mode": "mask",
            "final_answer": "limpo",
            "artifacts": [],
            "pii_counts": {"cpf": 2},
            "blocked": False,
        }):
            out = supervisor.node_guardrails_out(
                {"final_answer": "tem CPF 111.222.333-44", "artifacts": [], "warnings": []}
            )
        assert out["final_answer"] == "limpo"
        assert out["pii"]["pii_counts"] == {"cpf": 2}
        assert any("PII" in w for w in out["warnings"])
