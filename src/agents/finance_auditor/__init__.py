"""Agente FinanceAuditor — VoC e Análise de Fricção.

Realiza análise de Voice of Customer (VoC) e identifica pontos de fricção
nas interações de clientes, utilizando a tabela:
    silviosalviati.ds_inteligencia_analitica.analitiva_analise_ia

Arquitetura LangGraph (fan-out paralelo):
  fetch_data → [node_sentiment, node_friction, node_themes]
             → consolidate_metrics → report_generator
"""

from __future__ import annotations

from typing import Any

from src.agents.finance_auditor.graph import build_graph
from src.core.base_agent import BaseAgent
from src.shared.tools.llm import create_llm


class FinanceAuditorAgent(BaseAgent):
    """Agente de auditoria VoC e fricção para a Porto Seguro Holding."""

    def __init__(self) -> None:
        self._llm = create_llm()
        self._graph = build_graph(self._llm)

    # ------------------------------------------------------------------
    # Contrato BaseAgent
    # ------------------------------------------------------------------

    @property
    def agent_id(self) -> str:
        return "finance_auditor"

    @property
    def display_name(self) -> str:
        return "Finance AuditorIA"

    def analyze(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None = None,
    ) -> dict[str, Any]:
        """Executa o pipeline VoC e retorna relatório + métricas consolidadas."""
        initial_state = {
            "request_text": query,
            "project_id": project_id,
            "dataset_hint": dataset_hint,
        }

        final_state: dict[str, Any] = self._graph.invoke(initial_state)

        if final_state.get("error"):
            return {
                "status": "error",
                "error": final_state["error"],
                "markdown_report": final_state.get("markdown_report", ""),
                "warnings": final_state.get("warnings", []),
            }

        return {
            "status": "ok",
            "markdown_report": final_state.get("markdown_report", ""),
            "quality_score": final_state.get("quality_score", 0),
            "friction_score": final_state.get("friction_score", 0.0),
            "friction_label": final_state.get("friction_label", "N/A"),
            "sentiment_analysis": final_state.get("sentiment_result", {}),
            "friction_analysis": final_state.get("friction_result", {}),
            "themes_analysis": final_state.get("themes_result", {}),
            "total_records": final_state.get("total_records", 0),
            "date_range": {
                "start": final_state.get("date_filter_start", ""),
                "end": final_state.get("date_filter_end", ""),
            },
            "warnings": final_state.get("warnings", []),
        }

    def runtime_info(self) -> dict[str, str]:
        return {
            "agent_id": self.agent_id,
            "display_name": self.display_name,
            "graph_nodes": "fetch_data,node_sentiment,node_friction,node_themes,"
            "consolidate_metrics,report_generator",
            "source_table": "silviosalviati.ds_inteligencia_analitica.analitiva_analise_ia",
        }
