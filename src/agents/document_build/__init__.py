from __future__ import annotations

from typing import Any

from src.core.base_agent import BaseAgent


class DocumentBuildAgent(BaseAgent):
	@property
	def agent_id(self) -> str:
		return "document_build"

	@property
	def display_name(self) -> str:
		return "Document Build"

	def analyze(self, query: str, project_id: str, dataset_hint: str | None = None) -> dict[str, Any]:
		return {
			"status": "not_implemented",
			"message": "Document Build ainda nao foi implementado.",
			"input_preview": {
				"project_id": project_id,
				"dataset_hint": dataset_hint,
				"query_size": len(query or ""),
			},
		}

	def runtime_info(self) -> dict[str, str]:
		return {
			"provider": "n/a",
			"provider_label": "Nao aplicavel",
			"model": "n/a",
		}


__all__ = ["DocumentBuildAgent"]
