"""Testes unitários básicos do agente FinanceAuditor (Finance Voice IA).

Cobre apenas metadados do agente e roteamento via grafo Supervisor.
Os testes detalhados das capabilities e do grafo estão em
`tests/agents/test_finance_auditor_supervisor.py`.
"""

from __future__ import annotations

from unittest.mock import patch


class TestFinanceAuditorMetadata:
    def test_agent_id(self):
        from src.agents.finance_auditor import FinanceAuditorAgent

        with patch("src.agents.finance_auditor._create_llm"), \
             patch("src.agents.finance_auditor.build_supervisor_graph"):
            agent = FinanceAuditorAgent()
            assert agent.agent_id == "finance_auditor"

    def test_display_name(self):
        from src.agents.finance_auditor import FinanceAuditorAgent

        with patch("src.agents.finance_auditor._create_llm"), \
             patch("src.agents.finance_auditor.build_supervisor_graph"):
            agent = FinanceAuditorAgent()
            assert agent.display_name == "Finance Voice IA"

    def test_runtime_info_keys(self):
        from src.agents.finance_auditor import FinanceAuditorAgent

        with patch("src.agents.finance_auditor._create_llm"), \
             patch("src.agents.finance_auditor.build_supervisor_graph"):
            agent = FinanceAuditorAgent()
            info = agent.runtime_info()
            assert info["agent_id"] == "finance_auditor"
            assert info["display_name"] == "Finance Voice IA"
            assert "supervisor_nodes" in info
            assert "capabilities" in info
            # Não deve haver mais referências a domínio fixo (VoC/fricção).
            joined = " ".join(info.values()).lower()
            assert "voc" not in joined
            assert "fric" not in joined
            assert "sentiment" not in joined
            assert "analitica_analise_ia" not in joined
