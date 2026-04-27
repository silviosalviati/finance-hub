from __future__ import annotations

from typing import Any

from src.agents.query_build.graph import build_graph
from src.agents.query_build.state import QueryBuildState
from src.core.base_agent import BaseAgent
from src.shared.tools.llm import create_llm


class QueryBuildAgent(BaseAgent):
	def __init__(self) -> None:
		self._graph = None

	@property
	def agent_id(self) -> str:
		return "query_build"

	@property
	def display_name(self) -> str:
		return "Query Build"

	def _get_graph(self):
		if self._graph is None:
			self._graph = build_graph(create_llm())
		return self._graph

	def analyze(self, query: str, project_id: str, dataset_hint: str | None = None) -> dict[str, Any]:
		graph = self._get_graph()

		initial_state = QueryBuildState(
			request_text=query,
			project_id=project_id,
			dataset_hint=dataset_hint,
		)

		final_state = None
		for event in graph.stream(initial_state, stream_mode="values"):
			final_state = event

		if not final_state:
			raise RuntimeError("Nao foi possivel gerar SQL para a solicitacao.")

		dry = final_state.get("dry_run_generated")
		warnings = final_state.get("warnings") or []
		has_error = bool(final_state.get("error") or (dry and dry.error))

		return {
			"request_text": query,
			"generated_sql": final_state.get("generated_sql"),
			"explanation": final_state.get("explanation") or "",
			"assumptions": final_state.get("assumptions") or [],
			"warnings": warnings,
			"dry_run": {
				"bytes_processed": dry.bytes_processed if dry else None,
				"estimated_cost_usd": dry.estimated_cost_usd if dry else None,
				"error": dry.error if dry else None,
			},
			"sample_data": {
				"columns": final_state.get("sample_columns") or [],
				"rows": final_state.get("sample_rows") or [],
				"error": final_state.get("sample_error"),
			},
			"status": "ok" if final_state.get("generated_sql") and not has_error else "error",
			"error": final_state.get("error"),
		}

	def runtime_info(self) -> dict[str, str]:
		return {
			"provider": "shared",
			"provider_label": "Mesmo provider do runtime",
			"model": "Mesmo modelo configurado no .env",
		}


__all__ = ["QueryBuildAgent"]
