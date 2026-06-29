"""Testes da resolucao de dataset por gerencia via rotulos (labels) do BigQuery.

Cobre:
- `resolve_dataset_by_gerencia` (capabilities.py): match exato, fuzzy
  (absorve diferencas tipo "contas_a_receber" vs "contas_receber"), ausencia
  de match, datasets sem rotulo, chave de rotulo configuravel.
- `cap_text_to_sql`: fallback para `context["dataset_hint"]` quando o Planner
  nao informou `table_refs`/`dataset_ref` (dataset ja fixado pela gerencia).
- `node_planner`: o `dataset_hint` do estado e' incluido no prompt do LLM.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# resolve_dataset_by_gerencia
# ---------------------------------------------------------------------------

class TestResolveDatasetByGerencia:
    def test_match_exato(self):
        from src.agents.finance_auditor.capabilities import resolve_dataset_by_gerencia

        fake_datasets = [
            {"dataset_id": "ecommerce_saude", "labels": {"gerencia": "experiencia_cliente"}},
            {"dataset_id": "logistica_vendas", "labels": {"gerencia": "contas_receber"}},
        ]
        with patch(
            "src.agents.finance_auditor.capabilities.list_datasets_with_labels",
            return_value=fake_datasets,
        ):
            result = resolve_dataset_by_gerencia("proj", "experiencia_cliente")

        assert result == {
            "dataset_id": "ecommerce_saude",
            "gerencia": "experiencia_cliente",
            "label_key": "gerencia",
        }

    def test_match_fuzzy_absorve_stopwords(self):
        from src.agents.finance_auditor.capabilities import resolve_dataset_by_gerencia

        fake_datasets = [
            {"dataset_id": "logistica_vendas", "labels": {"gerencia": "contas_receber"}},
        ]
        with patch(
            "src.agents.finance_auditor.capabilities.list_datasets_with_labels",
            return_value=fake_datasets,
        ):
            # Slug do texto do cartao ("Contas a receber") tem uma palavra extra
            # ("a") em relacao ao valor real do rotulo — deve casar via fuzzy.
            result = resolve_dataset_by_gerencia("proj", "contas_a_receber")

        assert result is not None
        assert result["dataset_id"] == "logistica_vendas"

    def test_sem_match_retorna_none(self):
        from src.agents.finance_auditor.capabilities import resolve_dataset_by_gerencia

        fake_datasets = [
            {"dataset_id": "ecommerce_saude", "labels": {"gerencia": "experiencia_cliente"}},
        ]
        with patch(
            "src.agents.finance_auditor.capabilities.list_datasets_with_labels",
            return_value=fake_datasets,
        ):
            assert resolve_dataset_by_gerencia("proj", "cobranca") is None

    def test_dataset_sem_label_e_ignorado(self):
        from src.agents.finance_auditor.capabilities import resolve_dataset_by_gerencia

        fake_datasets = [{"dataset_id": "ds_inteligencia_analitica", "labels": {}}]
        with patch(
            "src.agents.finance_auditor.capabilities.list_datasets_with_labels",
            return_value=fake_datasets,
        ):
            assert resolve_dataset_by_gerencia("proj", "qualquer") is None

    def test_gerencia_vazia_retorna_none(self):
        from src.agents.finance_auditor.capabilities import resolve_dataset_by_gerencia

        assert resolve_dataset_by_gerencia("proj", "") is None
        assert resolve_dataset_by_gerencia("proj", "   ") is None

    def test_falha_ao_listar_datasets_retorna_none(self):
        from src.agents.finance_auditor.capabilities import resolve_dataset_by_gerencia

        with patch(
            "src.agents.finance_auditor.capabilities.list_datasets_with_labels",
            side_effect=RuntimeError("sem credenciais"),
        ):
            assert resolve_dataset_by_gerencia("proj", "experiencia_cliente") is None

    def test_label_key_configuravel(self):
        from src.agents.finance_auditor.capabilities import resolve_dataset_by_gerencia

        fake_datasets = [
            {"dataset_id": "ecommerce_saude", "labels": {"area": "experiencia_cliente"}},
        ]
        with patch(
            "src.agents.finance_auditor.capabilities.list_datasets_with_labels",
            return_value=fake_datasets,
        ), patch(
            "src.agents.finance_auditor.capabilities.get_runtime_config",
            return_value="area",
        ):
            result = resolve_dataset_by_gerencia("proj", "experiencia_cliente")

        assert result is not None
        assert result["dataset_id"] == "ecommerce_saude"
        assert result["label_key"] == "area"


# ---------------------------------------------------------------------------
# cap_text_to_sql — fallback para context["dataset_hint"]
# ---------------------------------------------------------------------------

class TestTextToSqlDatasetHintFallback:
    def test_usa_dataset_hint_do_contexto_quando_args_vazios(self):
        from src.agents.finance_auditor import capabilities

        tables = [
            {
                "table_id": "clientes",
                "full_name": "p.ecommerce_saude.clientes",
                "columns": ["id_cliente", "nome_completo"],
            },
        ]

        sql_resp = MagicMock()
        sql_resp.sql = "SELECT id_cliente FROM `p.ecommerce_saude.clientes` LIMIT 10"
        sql_struct = MagicMock()
        sql_struct.invoke.return_value = sql_resp
        llm = MagicMock()
        llm.with_structured_output.return_value = sql_struct

        fake_dry = MagicMock(error=None, bytes_processed=128, estimated_cost_usd=0.0)

        with patch.object(
            capabilities, "get_dataset_tables_metadata",
            return_value={"dataset_ref": "p.ecommerce_saude", "tables": tables},
        ), patch.object(capabilities, "get_table_schema", return_value="schema"), \
             patch.object(capabilities.rbac, "check_dataset", return_value=(True, "")), \
             patch.object(capabilities, "dry_run_query", return_value=fake_dry), \
             patch.object(capabilities, "execute_query_rows", return_value=([{"id_cliente": "1"}], 0)), \
             patch.object(capabilities, "get_runtime_config", return_value=str(5 * 1024 ** 3)):
            out = capabilities.cap_text_to_sql(
                {"natural_language": "quais sao os clientes?"},
                {"project_id": "p", "llm": llm, "dataset_hint": "p.ecommerce_saude"},
            )

        assert out["ok"] is True
        assert out["payload"]["table_refs"] == ["p.ecommerce_saude.clientes"]

    def test_sem_dataset_hint_no_contexto_continua_exigindo_args(self):
        from src.agents.finance_auditor.capabilities import cap_text_to_sql

        out = cap_text_to_sql(
            {"natural_language": "quantas linhas?"},
            {"project_id": "p", "llm": MagicMock()},
        )
        assert out["ok"] is False
        err = (out["error"] or "").lower()
        assert "table_refs" in err or "dataset_ref" in err

    def test_args_explicitos_tem_prioridade_sobre_dataset_hint(self):
        from src.agents.finance_auditor.capabilities import cap_text_to_sql

        out = cap_text_to_sql(
            {"natural_language": "q", "table_refs": ["ds.tbl"]},
            {"project_id": "p", "llm": MagicMock(), "dataset_hint": "p.outro_dataset"},
        )
        # table_refs invalido (sem 3 partes) — prova que o caminho usado foi o
        # de table_refs explicito, nao o fallback de dataset_hint.
        assert out["ok"] is False
        assert "inv" in (out["error"] or "").lower()


# ---------------------------------------------------------------------------
# node_planner — surfaceamento do dataset_hint no prompt
# ---------------------------------------------------------------------------

class TestPlannerDatasetHintContext:
    @staticmethod
    def _fake_invoke(_llm, messages, max_attempts=2):
        from src.agents.finance_auditor.supervisor_schemas import PlanResponse, PlanStep

        TestPlannerDatasetHintContext._captured = messages
        return PlanResponse(rationale="ok", steps=[PlanStep(capability="chat_answer", args={})])

    def test_dataset_hint_presente_e_incluido_no_prompt(self):
        from src.agents.finance_auditor import supervisor

        with patch.object(supervisor, "invoke_with_retry", side_effect=self._fake_invoke):
            supervisor.node_planner(
                {
                    "request_text": "quero saber o total",
                    "dataset_hint": "p.ecommerce_saude",
                    "guardrail_in_ok": True,
                },
                llm=MagicMock(),
            )

        human_msg = self._captured[1]
        assert "p.ecommerce_saude" in human_msg.content
        assert "bq_list_datasets" in human_msg.content

    def test_sem_dataset_hint_prompt_e_o_texto_original(self):
        from src.agents.finance_auditor import supervisor

        with patch.object(supervisor, "invoke_with_retry", side_effect=self._fake_invoke):
            supervisor.node_planner(
                {"request_text": "oi", "guardrail_in_ok": True},
                llm=MagicMock(),
            )

        human_msg = self._captured[1]
        assert human_msg.content == "oi"
