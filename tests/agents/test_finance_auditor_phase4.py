"""Testes da Fase 4 do Finance Voice IA — reflect, memória, forecast, multimodal, alerting."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Forecast
# ---------------------------------------------------------------------------

class TestForecast:
    def test_linear_regression_perfeita(self):
        from src.agents.finance_auditor.forecast import linear_regression

        r = linear_regression([1.0, 2.0, 3.0, 4.0, 5.0])
        assert r["slope"] == pytest.approx(1.0)
        assert r["intercept"] == pytest.approx(1.0)
        assert r["r2"] == pytest.approx(1.0)

    def test_project_basico(self):
        from src.agents.finance_auditor.forecast import project

        rows = [{"mes": f"2026-0{i}", "v": float(i)} for i in range(1, 6)]
        out = project(rows, value_column="v", horizon=3, time_column="mes")
        assert out["ok"] is True
        assert out["n_points"] == 5
        assert out["direction"] == "alta"
        assert len(out["forecasts"]) == 3
        # próximo ponto após x=4 (índices 0..4) deve ser y ≈ 6
        assert out["forecasts"][0]["y"] == pytest.approx(6.0, abs=1e-6)

    def test_project_serie_pequena(self):
        from src.agents.finance_auditor.forecast import project

        out = project([{"v": 1}], value_column="v")
        assert out["ok"] is False

    def test_project_coluna_inexistente(self):
        from src.agents.finance_auditor.forecast import project

        out = project([{"x": 1}, {"x": 2}], value_column="v")
        assert out["ok"] is False


# ---------------------------------------------------------------------------
# Multimodal
# ---------------------------------------------------------------------------

class TestMultimodal:
    def test_parse_csv_simples(self):
        import base64
        from src.agents.finance_auditor.multimodal import parse_csv

        csv = "id,nome\n1,A\n2,B\n3,C\n"
        b64 = base64.b64encode(csv.encode()).decode()
        out = parse_csv(b64)
        assert out["row_count"] == 3
        assert out["columns"] == ["id", "nome"]
        assert out["rows"][0] == {"id": "1", "nome": "A"}

    def test_parse_csv_base64_invalido(self):
        from src.agents.finance_auditor.multimodal import parse_csv

        with pytest.raises(ValueError):
            parse_csv("###não-é-base64###")

    def test_describe_image_sem_llm_falha(self):
        import base64
        from src.agents.finance_auditor.multimodal import describe_image_with_llm

        with pytest.raises(ValueError):
            describe_image_with_llm(base64.b64encode(b"x").decode(), "p", llm=None)


# ---------------------------------------------------------------------------
# Capability: forecast_simple
# ---------------------------------------------------------------------------

class TestCapForecast:
    def test_forecast_simple_usa_step_anterior(self):
        from src.agents.finance_auditor.capabilities import cap_forecast_simple

        rows = [{"mes": "2026-01", "v": i} for i in range(1, 11)]
        ctx = {"tool_results": [{"ok": True, "payload": {"rows": rows}}]}
        out = cap_forecast_simple(
            {"source_step_index": 0, "value_column": "v", "horizon": 3}, ctx
        )
        assert out["ok"] is True
        assert out["payload"]["direction"] == "alta"
        assert len(out["payload"]["forecasts"]) == 3
        # artifact deve trazer tabela com forecasts
        assert out["artifacts"][0]["type"] == "table"


# ---------------------------------------------------------------------------
# Capability: attachment_analyze (CSV)
# ---------------------------------------------------------------------------

class TestCapAttachment:
    def test_csv_anexo_e_lido(self):
        import base64
        from src.agents.finance_auditor.capabilities import cap_attachment_analyze

        csv = "a,b\n1,2\n3,4\n"
        attach = {"kind": "csv", "data": base64.b64encode(csv.encode()).decode(), "filename": "x.csv"}
        out = cap_attachment_analyze(
            {"attachment_index": 0}, {"attachments": [attach]}
        )
        assert out["ok"] is True
        assert out["payload"]["row_count"] == 2

    def test_sem_anexos_falha(self):
        from src.agents.finance_auditor.capabilities import cap_attachment_analyze

        out = cap_attachment_analyze({"attachment_index": 0}, {})
        assert out["ok"] is False
        assert "anexo" in (out["error"] or "").lower()

    def test_kind_invalido(self):
        from src.agents.finance_auditor.capabilities import cap_attachment_analyze

        out = cap_attachment_analyze(
            {"attachment_index": 0},
            {"attachments": [{"kind": "video", "data": "Zm9v"}]},
        )
        assert out["ok"] is False
        assert "kind" in (out["error"] or "").lower()


# ---------------------------------------------------------------------------
# Capabilities: org_fact_save / org_fact_recall
# ---------------------------------------------------------------------------

class TestCapOrgFacts:
    def test_save_sem_usuario_autenticado(self):
        from src.agents.finance_auditor.capabilities import cap_org_fact_save

        out = cap_org_fact_save({"fact_text": "x"}, {"user": {}})
        assert out["ok"] is False

    def test_save_e_recall(self):
        from src.agents.finance_auditor import capabilities

        with patch.object(capabilities.org_memory, "save_fact", return_value=10) as save_mock:
            out = capabilities.cap_org_fact_save(
                {"fact_text": "prefiro relatórios trimestrais", "tags": "frequencia"},
                {"user": {"username": "u1"}},
            )
        assert out["ok"] is True
        assert out["payload"]["id"] == 10
        save_mock.assert_called_once()

        facts = [{"id": 10, "scope": "user", "fact_text": "prefiro relatórios trimestrais",
                  "tags": "frequencia", "created_at": ""}]
        with patch.object(capabilities.org_memory, "recall", return_value=facts):
            out = capabilities.cap_org_fact_recall(
                {"query": "relatórios"},
                {"user": {"username": "u1"}, "request_text": "x"},
            )
        assert out["ok"] is True
        assert out["payload"]["fact_count"] == 1

    def test_scope_global_sem_admin_e_demovido(self):
        from src.agents.finance_auditor import capabilities

        captured = {}

        def fake_save(*, user_id, fact_text, tags, scope):
            captured["scope"] = scope
            return 1

        with patch.object(capabilities.org_memory, "save_fact", side_effect=fake_save):
            capabilities.cap_org_fact_save(
                {"fact_text": "y", "scope": "global"},
                {"user": {"username": "u1", "is_admin": False}},
            )
        assert captured["scope"] == "user"


# ---------------------------------------------------------------------------
# Reflect node
# ---------------------------------------------------------------------------

class TestReflectNode:
    def test_reflect_ignora_quando_tudo_ok_e_tem_resposta(self):
        from src.agents.finance_auditor import supervisor

        # Plano completo: text_to_sql é "answer-producing".
        out = supervisor.node_reflect(
            {
                "tool_results": [
                    {"capability": "bq_get_schema", "ok": True},
                    {"capability": "text_to_sql", "ok": True},
                ],
                "iteration": 1,
            },
            llm=MagicMock(),
        )
        assert out["reflect"]["is_valid"] is True
        assert out["reflect"]["suggested_steps"] == []

    def test_reflect_pula_quando_atinge_max_iter(self):
        from src.agents.finance_auditor import supervisor

        out = supervisor.node_reflect(
            {"tool_results": [{"ok": False, "error": "x"}], "iteration": 99},
            llm=MagicMock(),
        )
        assert out["reflect"]["suggested_steps"] == []

    def test_reflect_propoe_steps_quando_ha_falha(self):
        from src.agents.finance_auditor import supervisor
        from src.agents.finance_auditor.supervisor_schemas import (
            PlanStep,
            ReflectVerdict,
        )

        verdict = ReflectVerdict(
            is_valid=False,
            confidence=0.7,
            issues=["dataset não encontrado"],
            suggested_steps=[PlanStep(capability="bq_list_datasets")],
        )
        fake_llm = MagicMock()
        fake_llm.with_structured_output.return_value.invoke.return_value = verdict
        out = supervisor.node_reflect(
            {
                "request_text": "q",
                "plan": [{"capability": "bq_list_tables", "args": {"dataset_hint": "x"}}],
                "tool_results": [{"capability": "bq_list_tables", "ok": False, "error": "404"}],
                "iteration": 1,
            },
            llm=fake_llm,
        )
        assert out["reflect"]["is_valid"] is False
        assert out["reflect"]["suggested_steps"][0]["capability"] == "bq_list_datasets"

    def test_reflect_router_volta_para_router_quando_invalido(self):
        from src.agents.finance_auditor import supervisor

        edge = supervisor._reflect_router(
            {"reflect": {"is_valid": False, "suggested_steps": [{"capability": "x"}]},
             "iteration": 1}
        )
        assert edge == "router"

    def test_reflect_router_vai_composer_quando_max_iter(self):
        from src.agents.finance_auditor import supervisor

        edge = supervisor._reflect_router(
            {"reflect": {"is_valid": False, "suggested_steps": [{"capability": "x"}]},
             "iteration": 99}
        )
        assert edge == "composer"

    def test_apply_reflect_plan_acumula_steps(self):
        from src.agents.finance_auditor import supervisor

        out = supervisor.node_apply_reflect_plan(
            {
                "plan": [{"capability": "a"}],
                "reflect": {"suggested_steps": [{"capability": "b"}, {"capability": "c"}]},
            }
        )
        assert [s["capability"] for s in out["plan"]] == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Router preserva tool_results entre iterações
# ---------------------------------------------------------------------------

class TestRouterRetryPreservation:
    def test_router_nao_re_executa_steps_ja_concluidos(self):
        from src.agents.finance_auditor import capabilities as cmod
        from src.agents.finance_auditor.supervisor import node_router

        calls: list[str] = []

        def fake_a(args, ctx):
            calls.append("a")
            return {"ok": True, "payload": {}, "error": None, "artifacts": []}

        def fake_b(args, ctx):
            calls.append("b")
            return {"ok": True, "payload": {}, "error": None, "artifacts": []}

        with patch.dict(cmod.CAPABILITY_REGISTRY, {"cap_a": fake_a, "cap_b": fake_b}, clear=False):
            state = {
                "plan": [{"capability": "cap_a"}, {"capability": "cap_b"}],
                "tool_results": [
                    {"step_index": 0, "capability": "cap_a", "ok": True, "payload": {}, "error": None}
                ],
                "artifacts": [],
                "iteration": 1,
                "project_id": "p",
            }
            out = node_router(state, llm=MagicMock(), llm_creative=MagicMock())
        # cap_a NÃO deve ter sido invocada de novo; cap_b sim.
        assert calls == ["b"]
        assert len(out["tool_results"]) == 2
        assert out["iteration"] == 2


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------

class TestLateBindingPlaceholders:
    def test_resolve_project(self):
        from src.agents.finance_auditor.supervisor import _resolve_placeholders

        out = _resolve_placeholders({"x": "${PROJECT}.ds.t"}, [], "meu_proj")
        assert out == {"x": "meu_proj.ds.t"}

    def test_resolve_step_payload_simples(self):
        from src.agents.finance_auditor.supervisor import _resolve_placeholders

        prior = [
            {"ok": True, "payload": {"resolved_dataset": "ecommerce_saude"}},
        ]
        out = _resolve_placeholders(
            {"dataset_hint": "${step_0.payload.resolved_dataset}"},
            prior,
            "p",
        )
        assert out == {"dataset_hint": "ecommerce_saude"}

    def test_resolve_step_lista_indexada(self):
        from src.agents.finance_auditor.supervisor import _resolve_placeholders

        prior = [
            {"ok": True, "payload": {"tables": [{"table_id": "pedidos"}, {"table_id": "clientes"}]}},
        ]
        out = _resolve_placeholders(
            {"table_ref": "${PROJECT}.ds.${step_0.payload.tables[0].table_id}"},
            prior,
            "p",
        )
        assert out == {"table_ref": "p.ds.pedidos"}

    def test_step_falhou_nao_substitui(self):
        from src.agents.finance_auditor.supervisor import _resolve_placeholders

        prior = [{"ok": False, "payload": None}]
        out = _resolve_placeholders({"x": "${step_0.payload.foo}"}, prior, "")
        assert out == {"x": "${step_0.payload.foo}"}


class TestReflectIncompletePlan:
    def test_reflect_dispara_quando_plano_incompleto(self):
        from src.agents.finance_auditor import supervisor
        from src.agents.finance_auditor.supervisor_schemas import (
            PlanStep,
            ReflectVerdict,
        )

        verdict = ReflectVerdict(
            is_valid=False,
            confidence=0.7,
            issues=["plano não chegou ao text_to_sql"],
            suggested_steps=[PlanStep(capability="text_to_sql")],
        )
        fake_llm = MagicMock()
        fake_llm.with_structured_output.return_value.invoke.return_value = verdict
        state = {
            "request_text": "qual o total de vendas?",
            "plan": [{"capability": "bq_list_datasets"}],
            "tool_results": [
                {"capability": "bq_list_datasets", "ok": True,
                 "payload": {"datasets": ["x"]}, "error": None}
            ],
            "iteration": 1,
        }
        out = supervisor.node_reflect(state, llm=fake_llm)
        assert out["reflect"]["is_valid"] is False
        assert out["reflect"]["suggested_steps"][0]["capability"] == "text_to_sql"

    def test_reflect_aprova_quando_plano_completo(self):
        from src.agents.finance_auditor import supervisor

        out = supervisor.node_reflect(
            {
                "tool_results": [
                    {"capability": "text_to_sql", "ok": True,
                     "payload": {"rows": [{"n": 1}]}, "error": None}
                ],
                "iteration": 1,
            },
            llm=MagicMock(),
        )
        assert out["reflect"]["is_valid"] is True
        assert out["reflect"]["suggested_steps"] == []


class TestAlerting:
    def test_compare_operators(self):
        from src.agents.finance_auditor.alerting import _compare

        assert _compare(10, ">", 5) is True
        assert _compare(5, ">=", 5) is True
        assert _compare(5, "<", 5) is False
        assert _compare(5, "==", 5) is True

    def test_parse_threshold_invalido(self):
        from src.agents.finance_auditor.alerting import _parse_threshold

        assert _parse_threshold("") is None
        assert _parse_threshold("nope") is None
        assert _parse_threshold('{"op": "??"}') is None

    def test_parse_threshold_valido(self):
        from src.agents.finance_auditor.alerting import _parse_threshold

        out = _parse_threshold('{"column":"v","op":">=","value":10}')
        assert out["aggregate"] == "first"  # default
        assert out["op"] == ">="

    def test_aggregate(self):
        from src.agents.finance_auditor.alerting import _aggregate

        rows = [{"v": 1}, {"v": 2}, {"v": 3}]
        assert _aggregate(rows, "v", "max") == 3
        assert _aggregate(rows, "v", "sum") == 6
        assert _aggregate(rows, "v", "avg") == 2

    def test_run_alerts_dispara_quando_threshold_excedido(self):
        from src.agents.finance_auditor import alerting

        metrics = [
            {
                "key": "vendas",
                "name": "Vendas",
                "sql_template": "SELECT 100 AS v",
                "alert_threshold": '{"column":"v","op":">=","value":50,"aggregate":"max"}',
            }
        ]
        fake_exec = {"ok": True, "payload": {"rows": [{"v": 100}]}, "error": None, "artifacts": []}
        with patch.object(alerting, "list_finance_metrics", return_value=metrics), \
             patch.object(alerting, "_validate_and_run_sql", return_value=fake_exec):
            out = alerting.run_alerts(project_id="p", user={"is_admin": True})
        assert len(out) == 1
        assert out[0]["triggered"] is True
        assert out[0]["actual"] == 100.0

    def test_run_alerts_ignora_metrica_sem_threshold(self):
        from src.agents.finance_auditor import alerting

        metrics = [{"key": "x", "name": "X", "sql_template": "SELECT 1", "alert_threshold": ""}]
        with patch.object(alerting, "list_finance_metrics", return_value=metrics):
            out = alerting.run_alerts(project_id="p")
        assert out == []
