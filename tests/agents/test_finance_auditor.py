"""Testes unitários do agente FinanceAuditor.

Testa metadados, nós Python puro (sem LLM / BigQuery) e a consolidação.
Imports de graph/nodes são feitos localmente nos testes que precisam para
isolar possíveis falhas de dependências externas.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures compartilhadas
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_rows() -> list[dict[str, Any]]:
    """Amostra representativa de linhas da tabela de análise."""
    return [
        {
            "SENTIMENTO_CLIENTE": "NEGATIVO",
            "TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS": 360.0,
            "CONVERSA_E_RECHAMADA": "SIM",
            "PALAVRAS_CHAVE": "sinistro, demora, cancelamento",
            "ASSUNTO": "Sinistro auto",
        },
        {
            "SENTIMENTO_CLIENTE": "POSITIVO",
            "TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS": 45.0,
            "CONVERSA_E_RECHAMADA": "NAO",
            "PALAVRAS_CHAVE": "renovacao, desconto",
            "ASSUNTO": "Renovação de apólice",
        },
        {
            "SENTIMENTO_CLIENTE": "NEUTRO",
            "TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS": 120.0,
            "CONVERSA_E_RECHAMADA": "NAO",
            "PALAVRAS_CHAVE": "segunda via, boleto",
            "ASSUNTO": "2ª via de boleto",
        },
        {
            "SENTIMENTO_CLIENTE": "NEGATIVO",
            "TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS": 10.0,
            "CONVERSA_E_RECHAMADA": "SIM",
            "PALAVRAS_CHAVE": "reclamacao, atendimento",
            "ASSUNTO": "Reclamação de atendimento",
        },
        {
            "SENTIMENTO_CLIENTE": "POSITIVO",
            "TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS": 60.0,
            "CONVERSA_E_RECHAMADA": "NAO",
            "PALAVRAS_CHAVE": "elogio, assistencia",
            "ASSUNTO": "Elogio assistência 24h",
        },
    ]


# ---------------------------------------------------------------------------
# Metadados do agente
# ---------------------------------------------------------------------------

class TestFinanceAuditorMetadata:
    def test_agent_id(self):
        from src.agents.finance_auditor import FinanceAuditorAgent

        with patch("src.agents.finance_auditor.create_llm"), \
             patch("src.agents.finance_auditor.build_graph"):
            agent = FinanceAuditorAgent()
            assert agent.agent_id == "finance_auditor"

    def test_display_name(self):
        from src.agents.finance_auditor import FinanceAuditorAgent

        with patch("src.agents.finance_auditor.create_llm"), \
             patch("src.agents.finance_auditor.build_graph"):
            agent = FinanceAuditorAgent()
            assert agent.display_name == "Finance AuditorIA"

    def test_runtime_info_keys(self):
        from src.agents.finance_auditor import FinanceAuditorAgent

        with patch("src.agents.finance_auditor.create_llm"), \
             patch("src.agents.finance_auditor.build_graph"):
            agent = FinanceAuditorAgent()
            info = agent.runtime_info()
            assert "agent_id" in info
            assert "display_name" in info
            assert "graph_nodes" in info
            assert "source_table" in info
            assert "analitica_analise_ia" in info["source_table"]


# ---------------------------------------------------------------------------
# node_sentiment — análise Python pura, sem mock
# ---------------------------------------------------------------------------

class TestNodeSentiment:
    def test_counts_correctly(self, sample_rows):
        from src.agents.finance_auditor.nodes import node_sentiment

        result = node_sentiment({"raw_rows": sample_rows})
        sr = result["sentiment_result"]

        assert sr["counts"]["POSITIVO"] == 2
        assert sr["counts"]["NEGATIVO"] == 2
        assert sr["counts"]["NEUTRO"] == 1
        assert sr["total_sample"] == 5

    def test_distribution_sums_to_100(self, sample_rows):
        from src.agents.finance_auditor.nodes import node_sentiment

        result = node_sentiment({"raw_rows": sample_rows})
        distribution = result["sentiment_result"]["distribution"]
        total_pct = sum(v["pct"] for v in distribution.values())
        # permite margem de 1 pp por arredondamento
        assert abs(total_pct - 100.0) <= 1.0

    def test_empty_rows(self):
        from src.agents.finance_auditor.nodes import node_sentiment

        result = node_sentiment({"raw_rows": []})
        assert result["sentiment_result"]["total_sample"] == 0
        assert result["sentiment_result"]["dominant"] == "N/A"

    def test_unknown_sentiment_goes_to_outros(self):
        from src.agents.finance_auditor.nodes import node_sentiment

        rows = [{"SENTIMENTO_CLIENTE": "FRUSTRADO", "TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS": 0,
                 "CONVERSA_E_RECHAMADA": "NAO", "PALAVRAS_CHAVE": "", "ASSUNTO": ""}]
        result = node_sentiment({"raw_rows": rows})
        assert result["sentiment_result"]["counts"]["OUTROS"] == 1


# ---------------------------------------------------------------------------
# node_friction — análise Python pura, sem mock
# ---------------------------------------------------------------------------

class TestNodeFriction:
    def test_detects_friction_cases(self, sample_rows):
        from src.agents.finance_auditor.nodes import node_friction

        result = node_friction({"raw_rows": sample_rows})
        fr = result["friction_result"]
        # Linha 0: NEGATIVO + espera 360s > 300 + rechamada → fricção
        # Linha 3: NEGATIVO + rechamada → fricção
        # Total: 2 casos
        assert fr["friction_count"] == 2

    def test_friction_pct_calculation(self, sample_rows):
        from src.agents.finance_auditor.nodes import node_friction

        result = node_friction({"raw_rows": sample_rows})
        fr = result["friction_result"]
        expected_pct = round(2 / 5 * 100, 1)
        assert fr["friction_pct"] == expected_pct

    def test_no_negativo_no_friction(self):
        from src.agents.finance_auditor.nodes import node_friction

        rows = [
            {"SENTIMENTO_CLIENTE": "POSITIVO", "TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS": 999.0,
             "CONVERSA_E_RECHAMADA": "SIM", "PALAVRAS_CHAVE": "", "ASSUNTO": ""},
        ]
        result = node_friction({"raw_rows": rows})
        assert result["friction_result"]["friction_count"] == 0

    def test_empty_rows(self):
        from src.agents.finance_auditor.nodes import node_friction

        result = node_friction({"raw_rows": []})
        assert result["friction_result"]["friction_count"] == 0
        assert result["friction_result"]["friction_pct"] == 0.0


# ---------------------------------------------------------------------------
# consolidate_metrics — agrega resultados dos nós paralelos
# ---------------------------------------------------------------------------

class TestConsolidateMetrics:
    def _build_state(
        self, sentiment: dict, friction: dict, themes: dict
    ) -> dict[str, Any]:
        return {
            "sentiment_result": sentiment,
            "friction_result": friction,
            "themes_result": themes,
            "date_filter_start": "2025-03-01",
            "date_filter_end": "2025-03-31",
            "total_records": 1000,
        }

    def test_friction_label_critico(self):
        from src.agents.finance_auditor.nodes import consolidate_metrics

        state = self._build_state(
            sentiment={"total_sample": 100, "distribution": {}, "dominant": "NEGATIVO"},
            friction={"friction_count": 35, "breakdown": {}},
            themes={"themes": [], "insights": ""},
        )
        result = consolidate_metrics(state)
        assert result["friction_label"] == "CRÍTICO"
        assert result["friction_score"] == pytest.approx(0.35, abs=1e-4)

    def test_friction_label_alto(self):
        from src.agents.finance_auditor.nodes import consolidate_metrics

        state = self._build_state(
            sentiment={"total_sample": 100, "distribution": {}, "dominant": "NEGATIVO"},
            friction={"friction_count": 20, "breakdown": {}},
            themes={"themes": [], "insights": ""},
        )
        result = consolidate_metrics(state)
        assert result["friction_label"] == "ALTO"

    def test_friction_label_baixo(self):
        from src.agents.finance_auditor.nodes import consolidate_metrics

        state = self._build_state(
            sentiment={"total_sample": 100, "distribution": {}, "dominant": "POSITIVO"},
            friction={"friction_count": 2, "breakdown": {}},
            themes={"themes": [], "insights": ""},
        )
        result = consolidate_metrics(state)
        assert result["friction_label"] == "BAIXO"

    def test_consolidated_metrics_has_period(self):
        from src.agents.finance_auditor.nodes import consolidate_metrics

        state = self._build_state(
            sentiment={"total_sample": 50, "distribution": {}, "dominant": "NEUTRO"},
            friction={"friction_count": 5, "breakdown": {}},
            themes={"themes": [], "insights": ""},
        )
        result = consolidate_metrics(state)
        assert "2025-03-01" in result["consolidated_metrics"]["period"]
        assert "2025-03-31" in result["consolidated_metrics"]["period"]

    def test_zero_sample_no_division_error(self):
        from src.agents.finance_auditor.nodes import consolidate_metrics

        state = self._build_state(
            sentiment={"total_sample": 0, "distribution": {}, "dominant": "N/A"},
            friction={"friction_count": 0, "breakdown": {}},
            themes={"themes": [], "insights": ""},
        )
        result = consolidate_metrics(state)
        assert result["friction_score"] == pytest.approx(0.0)
        assert result["friction_label"] == "BAIXO"
