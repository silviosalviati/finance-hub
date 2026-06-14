from __future__ import annotations

import uuid
from typing import Any

from langgraph.types import Command

from src.agents.query_analyzer.graph import build_graph
from src.agents.query_analyzer.state import AgentState
from src.core.base_agent import BaseAgent
from src.shared.config import get_runtime_config
from src.shared.tools.llm import create_llm


class QueryAnalyzerAgent(BaseAgent):
    def __init__(self) -> None:
        self._graph = None

    @property
    def agent_id(self) -> str:
        return "query_analyzer"

    @property
    def display_name(self) -> str:
        return "Query Analyzer"

    def _get_graph(self):
        if self._graph is None:
            self._graph = build_graph(create_llm())
        return self._graph

    def analyze(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None = None,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        """Executa o pipeline de análise.

        Retorna `{"status": "ok", ...}` quando concluído normalmente ou
        `{"status": "awaiting_approval", "thread_id": ..., ...}` quando o
        pipeline pausou para aprovação humana dos antipadrões detectados.
        Use `resume(thread_id, decision)` para continuar.
        """
        graph = self._get_graph()
        tid = thread_id or str(uuid.uuid4())
        config = {"configurable": {"thread_id": tid}}

        initial_state = AgentState(
            original_query=query,
            project_id=project_id,
            dataset_hint=dataset_hint,
        )

        final_event: dict[str, Any] | None = None
        for event in graph.stream(initial_state, config=config, stream_mode="values"):
            final_event = event

        # Detecta pausa por interrupt() no nó await_human_approval
        snapshot = graph.get_state(config)
        if snapshot.next:
            return self._interrupted_response(tid, final_event)

        if not final_event or not final_event.get("report"):
            raise RuntimeError("Analise nao produziu relatorio.")

        return self._format_result(final_event)

    def resume(self, thread_id: str, human_decision: str) -> dict[str, Any]:
        """Retoma o pipeline após decisão humana.

        Args:
            thread_id: Identificador retornado pelo `analyze()` em estado
                       'awaiting_approval'.
            human_decision: 'approve' para prosseguir com otimização,
                            'skip' para ir direto ao relatório.
        """
        graph = self._get_graph()
        config = {"configurable": {"thread_id": thread_id}}

        final_event: dict[str, Any] | None = None
        for event in graph.stream(
            Command(resume=human_decision),
            config=config,
            stream_mode="values",
        ):
            final_event = event

        if not final_event or not final_event.get("report"):
            raise RuntimeError("Analise nao produziu relatorio apos retomada.")

        return self._format_result(final_event)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _interrupted_response(
        self,
        thread_id: str,
        last_event: dict[str, Any] | None,
    ) -> dict[str, Any]:
        antipatterns_raw = (last_event or {}).get("antipatterns") or []
        antipatterns_data = [
            ap.model_dump() if hasattr(ap, "model_dump") else ap
            for ap in antipatterns_raw
        ]
        dry = (last_event or {}).get("dry_run_original")
        return {
            "status": "awaiting_approval",
            "thread_id": thread_id,
            "needs_optimization": bool((last_event or {}).get("needs_optimization")),
            "antipatterns": antipatterns_data,
            "bytes_processed": dry.bytes_processed if dry and not dry.error else None,
            "estimated_cost_usd": dry.estimated_cost_usd if dry and not dry.error else None,
        }

    def _format_result(self, final_event: dict[str, Any]) -> dict[str, Any]:
        report = final_event["report"]
        dry_orig = final_event.get("dry_run_original")
        dry_opt = final_event.get("dry_run_optimized")

        return {
            "status": "ok",
            "efficiency_score": report.efficiency_score,
            "grade": report.grade,
            "summary": report.summary,
            "antipatterns": [
                {
                    "pattern": ap.pattern,
                    "description": ap.description,
                    "severity": ap.severity,
                    "line_hint": ap.line_hint,
                    "suggestion": ap.suggestion,
                }
                for ap in report.antipatterns_found
            ],
            "optimized_query": report.optimized_query,
            "bytes_original": dry_orig.bytes_processed if dry_orig else None,
            "bytes_optimized": dry_opt.bytes_processed if dry_opt else None,
            "cost_original_usd": dry_orig.estimated_cost_usd if dry_orig else None,
            "cost_optimized_usd": dry_opt.estimated_cost_usd if dry_opt else None,
            "bytes_saved": report.bytes_saved,
            "cost_saved_usd": report.cost_saved_usd,
            "savings_pct": report.savings_pct,
            "recommendations": report.recommendations,
            "power_bi_tips": report.power_bi_tips,
            "applied_optimizations": report.applied_optimizations,
            "dry_run_error": dry_orig.error if dry_orig else None,
        }

    def runtime_info(self) -> dict[str, str]:
        provider = get_runtime_config("LLM_PROVIDER", "vertexai").lower()

        if provider == "vertexai":
            return {
                "provider": "vertexai",
                "provider_label": "Vertex AI",
                "model": get_runtime_config("VERTEXAI_MODEL", "gemini-2.5-flash"),
            }

        return {
            "provider": provider,
            "provider_label": "Provider desconhecido",
            "model": "nao definido",
        }


__all__ = ["QueryAnalyzerAgent"]
