"""Finance Voice IA — assistente analítico genérico sobre BigQuery.

Único modo de execução: Supervisor + Specialists.

    guardrails_in → persona_resolver → planner → router → composer → guardrails_out

Sem domínio fixo. As capabilities são descobertas e encadeadas dinamicamente
pelo Planner conforme a pergunta do usuário.
"""

from __future__ import annotations

from typing import Any

from src.agents.finance_auditor.supervisor import build_supervisor_graph
from src.core.base_agent import BaseAgent
from src.shared.config import get_runtime_config
from src.shared.tools.llm import create_llm as _create_llm


class FinanceAuditorAgent(BaseAgent):
    """Agente conversacional de análise de dados."""

    def __init__(self) -> None:
        self._graph = None

    @property
    def agent_id(self) -> str:
        return "finance_auditor"

    @property
    def display_name(self) -> str:
        return "Finance Voice IA"

    def _get_graph(self):
        if self._graph is None:
            llm = _create_llm()
            t_creative = float(get_runtime_config("VERTEXAI_TEMPERATURE_CREATIVE", "0.3"))
            llm_creative = _create_llm(temperature=t_creative)
            self._graph = build_supervisor_graph(llm=llm, llm_creative=llm_creative)
        return self._graph

    def analyze(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None = None,
        user_profile: dict[str, Any] | None = None,
        user: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Executa o grafo Supervisor e devolve um dict compatível com o frontend."""
        graph = self._get_graph()

        u = user or {}
        initial_state = {
            "request_text": query,
            "project_id": project_id,
            "dataset_hint": dataset_hint,
            "user_profile": user_profile or {},
            "user_id": str(u.get("username") or u.get("user_id") or ""),
            "user": u,
            "attachments": list(attachments or []),
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
            "response_mode": "analysis",
            "persona": final_state.get("persona", ""),
            "plan": final_state.get("plan", []),
            "plan_rationale": final_state.get("plan_rationale", ""),
            "tool_results": final_state.get("tool_results", []),
            "artifacts": final_state.get("artifacts", []),
            # Campos compatíveis com o frontend atual:
            "markdown_report": final_state.get("final_answer", ""),
            "chat_answer": final_state.get("final_answer", ""),
            "warnings": final_state.get("warnings", []),
            "pii": final_state.get("pii", {}),
            "audit_id": final_state.get("audit_id"),
        }

    def runtime_info(self) -> dict[str, str]:
        return {
            "agent_id": self.agent_id,
            "display_name": self.display_name,
            "supervisor_nodes": (
                "guardrails_in,persona_resolver,response_mode_resolver,planner,"
                "router,composer,audit,guardrails_out"
            ),
            "capabilities": (
                "bq_list_datasets,bq_list_tables,bq_get_schema,bq_query,"
                "text_to_sql,stats_describe,viz_spec,"
                "metric_lookup,metric_execute,chat_answer"
            ),
        }
