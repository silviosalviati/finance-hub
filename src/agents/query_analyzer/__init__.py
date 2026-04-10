from __future__ import annotations

from typing import Any

from src.agents.query_analyzer.graph import build_graph
from src.agents.query_analyzer.state import AgentState
from src.core.base_agent import BaseAgent
from src.shared.config import HF_MODEL_ID, LLM_PROVIDER, OPENAI_MODEL, VERTEXAI_MODEL
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
            llm = create_llm()
            self._graph = build_graph(llm)
        return self._graph

    def analyze(self, query: str, project_id: str, dataset_hint: str | None = None) -> dict[str, Any]:
        graph = self._get_graph()

        initial_state = AgentState(
            original_query=query,
            project_id=project_id,
            dataset_hint=dataset_hint,
        )

        final_state = None
        for event in graph.stream(initial_state, stream_mode="values"):
            final_state = event

        if not final_state or not final_state.get("report"):
            raise RuntimeError("Analise nao produziu relatorio.")

        report = final_state["report"]
        dry_orig = final_state.get("dry_run_original")
        dry_opt = final_state.get("dry_run_optimized")

        return {
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
        provider = LLM_PROVIDER.lower()

        if provider == "openai":
            return {
                "provider": "openai",
                "provider_label": "OpenAI",
                "model": OPENAI_MODEL or "nao definido",
            }

        if provider == "vertexai":
            return {
                "provider": "vertexai",
                "provider_label": "Vertex AI",
                "model": VERTEXAI_MODEL or "nao definido",
            }

        if provider == "huggingface":
            return {
                "provider": "huggingface",
                "provider_label": "Hugging Face",
                "model": HF_MODEL_ID or "nao definido",
            }

        return {
            "provider": provider,
            "provider_label": "Provider desconhecido",
            "model": "nao definido",
        }


__all__ = ["QueryAnalyzerAgent"]
