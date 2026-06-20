"""Testes do Response Mode (análise profunda) e da escolha automática de
gráfico no Finance Voice IA.

Cobre:
- `detect_response_mode` (response_mode.py): reconhece pedidos de análise
  profunda/causa raiz/diagnóstico e cai para o modo padrão no resto.
- `node_response_mode_resolver` (supervisor.py): grava o modo no estado.
- `node_planner`: injeta o bloco de contexto de análise profunda quando o
  modo está ativo (mesmo padrão já usado para `dataset_hint`).
- `node_composer`: combina `persona_block` + `mode_block` no system prompt.
- `cap_viz_spec` (capabilities.py): heurística de `chart_type` automático.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# detect_response_mode
# ---------------------------------------------------------------------------

class TestDetectResponseMode:
    def test_analise_profunda_por_palavra_chave(self):
        from src.agents.finance_auditor.response_mode import (
            RESPONSE_MODE_ANALISE_PROFUNDA,
            detect_response_mode,
        )

        assert detect_response_mode("faça uma análise profunda da queda de receita") == RESPONSE_MODE_ANALISE_PROFUNDA
        assert detect_response_mode("qual a causa raiz do aumento de cancelamentos?") == RESPONSE_MODE_ANALISE_PROFUNDA
        assert detect_response_mode("quero um diagnóstico completo do mês") == RESPONSE_MODE_ANALISE_PROFUNDA
        assert detect_response_mode("monte um plano de ação para isso") == RESPONSE_MODE_ANALISE_PROFUNDA

    def test_padrao_para_pergunta_comum(self):
        from src.agents.finance_auditor.response_mode import (
            RESPONSE_MODE_PADRAO,
            detect_response_mode,
        )

        assert detect_response_mode("quanto vendemos em maio?") == RESPONSE_MODE_PADRAO
        assert detect_response_mode("") == RESPONSE_MODE_PADRAO
        assert detect_response_mode("liste os top 10 clientes") == RESPONSE_MODE_PADRAO

    def test_ignora_acentuacao(self):
        from src.agents.finance_auditor.response_mode import (
            RESPONSE_MODE_ANALISE_PROFUNDA,
            detect_response_mode,
        )

        assert detect_response_mode("ANALISE PROFUNDA da inadimplencia") == RESPONSE_MODE_ANALISE_PROFUNDA


class TestResponseModePrompts:
    def test_prompt_padrao_e_vazio(self):
        from src.agents.finance_auditor.response_mode import (
            RESPONSE_MODE_PADRAO,
            get_response_mode_prompt,
        )

        assert get_response_mode_prompt(RESPONSE_MODE_PADRAO) == ""

    def test_prompt_analise_profunda_tem_as_5_secoes(self):
        from src.agents.finance_auditor.response_mode import (
            RESPONSE_MODE_ANALISE_PROFUNDA,
            get_response_mode_prompt,
        )

        block = get_response_mode_prompt(RESPONSE_MODE_ANALISE_PROFUNDA)
        for heading in (
            "O que aconteceu?",
            "Por que aconteceu?",
            "Qual o impacto?",
            "O que fazer?",
            "O que priorizar?",
            "Próximas perguntas sugeridas",
        ):
            assert heading in block


# ---------------------------------------------------------------------------
# node_response_mode_resolver
# ---------------------------------------------------------------------------

class TestResponseModeResolverNode:
    def test_grava_modo_analise_profunda_no_estado(self):
        from src.agents.finance_auditor.supervisor import node_response_mode_resolver
        from src.agents.finance_auditor.response_mode import RESPONSE_MODE_ANALISE_PROFUNDA

        out = node_response_mode_resolver({"request_text": "qual a causa raiz da queda?"})
        assert out == {"response_mode": RESPONSE_MODE_ANALISE_PROFUNDA}

    def test_grava_modo_padrao_no_estado(self):
        from src.agents.finance_auditor.supervisor import node_response_mode_resolver
        from src.agents.finance_auditor.response_mode import RESPONSE_MODE_PADRAO

        out = node_response_mode_resolver({"request_text": "quanto vendemos ontem?"})
        assert out == {"response_mode": RESPONSE_MODE_PADRAO}


# ---------------------------------------------------------------------------
# node_planner — contexto de análise profunda
# ---------------------------------------------------------------------------

class TestPlannerResponseModeContext:
    @staticmethod
    def _fake_invoke(_llm, messages, max_attempts=2):
        from src.agents.finance_auditor.supervisor_schemas import PlanResponse, PlanStep

        TestPlannerResponseModeContext._captured = messages
        return PlanResponse(rationale="ok", steps=[PlanStep(capability="chat_answer", args={})])

    def test_contexto_e_incluido_quando_modo_e_analise_profunda(self):
        from src.agents.finance_auditor import supervisor
        from src.agents.finance_auditor.response_mode import RESPONSE_MODE_ANALISE_PROFUNDA

        with patch.object(supervisor, "invoke_with_retry", side_effect=self._fake_invoke):
            supervisor.node_planner(
                {
                    "request_text": "causa raiz da queda de receita",
                    "response_mode": RESPONSE_MODE_ANALISE_PROFUNDA,
                    "guardrail_in_ok": True,
                },
                llm=MagicMock(),
            )

        human_msg = self._captured[1]
        assert "ANÁLISE PROFUNDA" in human_msg.content
        assert "stats_describe" in human_msg.content

    def test_contexto_ausente_no_modo_padrao(self):
        from src.agents.finance_auditor import supervisor
        from src.agents.finance_auditor.response_mode import RESPONSE_MODE_PADRAO

        with patch.object(supervisor, "invoke_with_retry", side_effect=self._fake_invoke):
            supervisor.node_planner(
                {
                    "request_text": "quanto vendemos ontem?",
                    "response_mode": RESPONSE_MODE_PADRAO,
                    "guardrail_in_ok": True,
                },
                llm=MagicMock(),
            )

        human_msg = self._captured[1]
        assert human_msg.content == "quanto vendemos ontem?"

    def test_contextos_de_dataset_e_modo_combinam(self):
        from src.agents.finance_auditor import supervisor
        from src.agents.finance_auditor.response_mode import RESPONSE_MODE_ANALISE_PROFUNDA

        with patch.object(supervisor, "invoke_with_retry", side_effect=self._fake_invoke):
            supervisor.node_planner(
                {
                    "request_text": "causa raiz da queda",
                    "dataset_hint": "p.ecommerce_saude",
                    "response_mode": RESPONSE_MODE_ANALISE_PROFUNDA,
                    "guardrail_in_ok": True,
                },
                llm=MagicMock(),
            )

        human_msg = self._captured[1]
        assert "p.ecommerce_saude" in human_msg.content
        assert "ANÁLISE PROFUNDA" in human_msg.content


# ---------------------------------------------------------------------------
# node_composer — mode_block no system prompt
# ---------------------------------------------------------------------------

class TestComposerModeBlock:
    @staticmethod
    def _fake_invoke(_llm, messages, max_attempts=2):
        TestComposerModeBlock._captured = messages
        response = MagicMock()
        response.content = "resposta final"
        return response

    def test_modo_analise_profunda_aparece_no_system_prompt(self):
        from src.agents.finance_auditor import supervisor
        from src.agents.finance_auditor.response_mode import RESPONSE_MODE_ANALISE_PROFUNDA

        with patch.object(supervisor, "invoke_with_retry", side_effect=self._fake_invoke):
            out = supervisor.node_composer(
                {
                    "request_text": "causa raiz da queda de receita",
                    "response_mode": RESPONSE_MODE_ANALISE_PROFUNDA,
                    "tool_results": [],
                },
                llm=MagicMock(),
            )

        system_msg = self._captured[0]
        assert "O que aconteceu?" in system_msg.content
        assert out["final_answer"] == "resposta final"

    def test_modo_padrao_nao_inclui_secoes_de_analise_profunda(self):
        from src.agents.finance_auditor import supervisor
        from src.agents.finance_auditor.response_mode import RESPONSE_MODE_PADRAO

        with patch.object(supervisor, "invoke_with_retry", side_effect=self._fake_invoke):
            supervisor.node_composer(
                {
                    "request_text": "quanto vendemos ontem?",
                    "response_mode": RESPONSE_MODE_PADRAO,
                    "tool_results": [],
                },
                llm=MagicMock(),
            )

        system_msg = self._captured[0]
        assert "O que aconteceu?" not in system_msg.content


# ---------------------------------------------------------------------------
# cap_viz_spec — escolha automática de chart_type
# ---------------------------------------------------------------------------

class TestVizSpecChartTypeHeuristic:
    def test_serie_temporal_sugere_line(self):
        from src.agents.finance_auditor.capabilities import cap_viz_spec

        rows = [{"mes": "2025-01-01", "receita": 100}, {"mes": "2025-02-01", "receita": 120}]
        out = cap_viz_spec({"rows": rows, "x": "mes", "y": "receita"}, {})
        assert out["ok"] is True
        assert out["payload"]["chart_type"] == "line"
        assert out["payload"]["auto_selected"] is True

    def test_duas_quantitativas_sugere_point(self):
        from src.agents.finance_auditor.capabilities import cap_viz_spec

        rows = [{"idade": 30, "gasto": 100}, {"idade": 40, "gasto": 200}]
        out = cap_viz_spec({"rows": rows, "x": "idade", "y": "gasto"}, {})
        assert out["ok"] is True
        assert out["payload"]["chart_type"] == "point"

    def test_categorica_sugere_bar(self):
        from src.agents.finance_auditor.capabilities import cap_viz_spec

        rows = [{"categoria": "A", "total": 10}, {"categoria": "B", "total": 20}]
        out = cap_viz_spec({"rows": rows, "x": "categoria", "y": "total"}, {})
        assert out["ok"] is True
        assert out["payload"]["chart_type"] == "bar"

    def test_chart_type_explicito_e_respeitado(self):
        from src.agents.finance_auditor.capabilities import cap_viz_spec

        rows = [{"categoria": "A", "total": 10}, {"categoria": "B", "total": 20}]
        out = cap_viz_spec({"rows": rows, "x": "categoria", "y": "total", "chart_type": "arc"}, {})
        assert out["ok"] is True
        assert out["payload"]["chart_type"] == "arc"
        assert out["payload"]["auto_selected"] is False
