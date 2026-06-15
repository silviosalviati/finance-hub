"""Agente FinanceAuditor — Finance Voice IA.

Duas topologias coexistem, selecionadas por runtime config
``FINANCE_AUDITOR_MODE``:

- ``legacy`` (padrão): pipeline fixo VoC + Fricção
    fetch_data → [node_sentiment, node_friction, node_themes]
               → consolidate_metrics → report_generator

- ``supervisor``: novo grafo Supervisor + Specialists (fase 1)
    guardrails_in → persona_resolver → planner → router → composer → guardrails_out

O agent_id e o display_name são preservados em ambos os modos para não quebrar
o frontend nem as rotas existentes.
"""

from __future__ import annotations

from typing import Any

from src.agents.finance_auditor.graph import build_graph
from src.agents.finance_auditor.supervisor import build_supervisor_graph
from src.core.base_agent import BaseAgent
from src.shared.config import get_runtime_config
from src.shared.tools.llm import create_llm as _create_llm

MODE_LEGACY = "legacy"
MODE_SUPERVISOR = "supervisor"
_VALID_MODES = {MODE_LEGACY, MODE_SUPERVISOR}


class FinanceAuditorAgent(BaseAgent):
    """Agente de auditoria VoC e fricção para a Porto Seguro Holding."""

    def __init__(self) -> None:
        self._graph = None
        self._supervisor_graph = None

    # ------------------------------------------------------------------
    # Contrato BaseAgent
    # ------------------------------------------------------------------

    @property
    def agent_id(self) -> str:
        return "finance_auditor"

    @property
    def display_name(self) -> str:
        return "Finance Voice IA"

    # ------------------------------------------------------------------
    # Construção preguiçosa dos grafos
    # ------------------------------------------------------------------

    def _get_graph(self):
        if self._graph is None:
            llm = _create_llm()
            t_creative = float(get_runtime_config("VERTEXAI_TEMPERATURE_CREATIVE", "0.3"))
            llm_creative = _create_llm(temperature=t_creative)
            self._graph = build_graph(llm, llm_creative=llm_creative)
        return self._graph

    def _get_supervisor_graph(self):
        if self._supervisor_graph is None:
            llm = _create_llm()
            t_creative = float(get_runtime_config("VERTEXAI_TEMPERATURE_CREATIVE", "0.3"))
            llm_creative = _create_llm(temperature=t_creative)
            self._supervisor_graph = build_supervisor_graph(
                llm=llm,
                llm_creative=llm_creative,
                legacy_agent=self,
            )
        return self._supervisor_graph

    @staticmethod
    def _resolve_mode() -> str:
        mode = (get_runtime_config("FINANCE_AUDITOR_MODE", MODE_LEGACY) or MODE_LEGACY).strip().lower()
        return mode if mode in _VALID_MODES else MODE_LEGACY

    # ------------------------------------------------------------------
    # Entry-point principal
    # ------------------------------------------------------------------

    def analyze(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None = None,
        user_profile: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Executa a topologia ativa (legacy ou supervisor) e devolve um dict
        compatível com o contrato consumido por ``src/api/routes/agents.py``.
        """
        if self._resolve_mode() == MODE_SUPERVISOR:
            return self._analyze_supervisor(query, project_id, dataset_hint, user_profile or {})
        return self.legacy_analyze(query, project_id, dataset_hint)

    # ------------------------------------------------------------------
    # Legacy — pipeline VoC fixo (exposto como API pública para a capability
    # voc_report do Supervisor reusar este mesmo agente)
    # ------------------------------------------------------------------

    def legacy_analyze(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None = None,
    ) -> dict[str, Any]:
        """Executa o pipeline VoC e retorna relatório + métricas consolidadas."""
        graph = self._get_graph()

        initial_state = {
            "request_text": query,
            "project_id": project_id,
            "dataset_hint": dataset_hint,
        }

        final_state: dict[str, Any] | None = None
        for event in graph.stream(initial_state, stream_mode="values"):
            final_state = event

        if not final_state:
            return {
                "status": "error",
                "error": "Pipeline não produziu resultado.",
                "markdown_report": "",
                "warnings": [],
            }

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
            "operations_analyzed": final_state.get("operations_analyzed", []),
            "date_range": {
                "start": final_state.get("date_filter_start", ""),
                "end": final_state.get("date_filter_end", ""),
            },
            "warnings": final_state.get("warnings", []),
        }

    # ------------------------------------------------------------------
    # Supervisor — novo grafo (fase 1)
    # ------------------------------------------------------------------

    def _analyze_supervisor(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None,
        user_profile: dict[str, Any],
    ) -> dict[str, Any]:
        graph = self._get_supervisor_graph()

        initial_state = {
            "request_text": query,
            "project_id": project_id,
            "dataset_hint": dataset_hint,
            "user_profile": user_profile,
        }

        final_state: dict[str, Any] | None = None
        for event in graph.stream(initial_state, stream_mode="values"):
            final_state = event

        if not final_state:
            return {
                "status": "error",
                "error": "Supervisor não produziu resultado.",
                "markdown_report": "",
                "warnings": [],
            }

        if final_state.get("error"):
            return {
                "status": "error",
                "error": final_state["error"],
                "markdown_report": final_state.get("final_answer", ""),
                "warnings": final_state.get("warnings", []),
            }

        return {
            "status": "ok",
            "mode": MODE_SUPERVISOR,
            "persona": final_state.get("persona", ""),
            "plan": final_state.get("plan", []),
            "plan_rationale": final_state.get("plan_rationale", ""),
            "tool_results": final_state.get("tool_results", []),
            "artifacts": final_state.get("artifacts", []),
            # campos compatíveis com o frontend atual (chat / analysis)
            "markdown_report": final_state.get("final_answer", ""),
            "chat_answer": final_state.get("final_answer", ""),
            "warnings": final_state.get("warnings", []),
        }

    # ------------------------------------------------------------------
    # Metadados
    # ------------------------------------------------------------------

    def runtime_info(self) -> dict[str, str]:
        return {
            "agent_id": self.agent_id,
            "display_name": self.display_name,
            "mode": self._resolve_mode(),
            "graph_nodes": "fetch_data,node_sentiment,node_friction,node_themes,"
            "consolidate_metrics,report_generator",
            "supervisor_nodes": "guardrails_in,persona_resolver,planner,router,"
            "composer,guardrails_out",
            "source_table": "silviosalviati.ds_inteligencia_analitica.analitica_analise_ia",
        }
