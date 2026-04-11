from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class DocumentBuildState(BaseModel):
	request_text: str
	project_id: str
	dataset_hint: Optional[str] = None
	input_context: dict[str, Any] = Field(default_factory=dict)

	doc_type: str = "documentacao_tecnica"
	title: str = ""
	summary: str = ""
	audience: str = ""
	objective: str = ""
	frequency: str = ""
	table_name: str = ""
	table_path: str = ""
	mermaid_diagram: str = ""

	sections: list[dict[str, str]] = Field(default_factory=list)
	data_dictionary: list[dict[str, str]] = Field(default_factory=list)
	typing_notes: list[str] = Field(default_factory=list)
	assumptions: list[str] = Field(default_factory=list)
	risks: list[str] = Field(default_factory=list)
	acceptance_checklist: list[str] = Field(default_factory=list)
	next_steps: list[str] = Field(default_factory=list)
	warnings: list[str] = Field(default_factory=list)
	governance: dict[str, Any] = Field(default_factory=dict)
	pending_technical: list[str] = Field(default_factory=list)

	metadata: dict[str, Any] = Field(default_factory=dict)
	real_schema: dict[str, Any] = Field(default_factory=dict)
	dataplex_context: dict[str, Any] = Field(default_factory=dict)
	dbt_context: dict[str, Any] = Field(default_factory=dict)
	artifacts_context: dict[str, Any] = Field(default_factory=dict)
	draft_context: dict[str, Any] = Field(default_factory=dict)
	markdown_document: str = ""
	quality_score: int = 0
	output_context: dict[str, Any] = Field(default_factory=dict)

	error: Optional[str] = None
