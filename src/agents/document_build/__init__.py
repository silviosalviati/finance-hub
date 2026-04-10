from __future__ import annotations

from typing import Any

from src.agents.document_build.graph import build_graph
from src.agents.document_build.state import DocumentBuildState
from src.core.base_agent import BaseAgent
from src.shared.tools.llm import create_llm


class DocumentBuildAgent(BaseAgent):
	def __init__(self) -> None:
		self._graph = None

	@property
	def agent_id(self) -> str:
		return "document_build"

	@property
	def display_name(self) -> str:
		return "Document Build"

	def _get_graph(self):
		if self._graph is None:
			self._graph = build_graph(create_llm())
		return self._graph

	def analyze(self, query: str, project_id: str, dataset_hint: str | None = None) -> dict[str, Any]:
		graph = self._get_graph()

		initial_state = DocumentBuildState(
			request_text=query,
			project_id=project_id,
			dataset_hint=dataset_hint,
		)

		final_state = None
		for event in graph.stream(initial_state, stream_mode="values"):
			final_state = event

		if not final_state:
			raise RuntimeError("Nao foi possivel gerar a documentacao.")

		error = final_state.get("error")
		if error:
			return {
				"status": "error",
				"error": error,
				"warnings": final_state.get("warnings") or [],
			}

		return {
			"status": "ok",
			"title": final_state.get("title") or "Documentacao Tecnica",
			"doc_type": final_state.get("doc_type") or "documentacao_funcional",
			"summary": final_state.get("summary") or "",
			"audience": final_state.get("audience") or "",
			"objective": final_state.get("objective") or "",
			"frequency": final_state.get("frequency") or "",
			"sections": final_state.get("sections") or [],
			"data_dictionary": final_state.get("data_dictionary") or [],
			"assumptions": final_state.get("assumptions") or [],
			"risks": final_state.get("risks") or [],
			"acceptance_checklist": final_state.get("acceptance_checklist") or [],
			"next_steps": final_state.get("next_steps") or [],
			"warnings": final_state.get("warnings") or [],
			"governance": final_state.get("governance") or {},
			"quality_score": final_state.get("quality_score") or 0,
			"markdown_document": final_state.get("markdown_document") or "",
			"metadata": final_state.get("metadata") or {},
		}

	def runtime_info(self) -> dict[str, str]:
		return {
			"provider": "shared",
			"provider_label": "Mesmo provider do runtime",
			"model": "Mesmo modelo configurado no .env",
		}


__all__ = ["DocumentBuildAgent"]
