from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class DocumentBuildState(BaseModel):
	request_text: str
	project_id: str
	dataset_hint: Optional[str] = None

	doc_type: str = "documentacao_tecnica"
	title: str = ""
	summary: str = ""
	audience: str = ""

	sections: list[dict[str, str]] = Field(default_factory=list)
	assumptions: list[str] = Field(default_factory=list)
	risks: list[str] = Field(default_factory=list)
	acceptance_checklist: list[str] = Field(default_factory=list)
	next_steps: list[str] = Field(default_factory=list)
	warnings: list[str] = Field(default_factory=list)

	metadata: dict[str, Any] = Field(default_factory=dict)
	markdown_document: str = ""
	quality_score: int = 0

	error: Optional[str] = None
