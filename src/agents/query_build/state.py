from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from src.shared.tools.schemas import DryRunResult


class QueryBuildState(BaseModel):
	request_text: str
	project_id: str
	dataset_hint: Optional[str] = None

	generated_sql: Optional[str] = None
	explanation: str = ""
	assumptions: list[str] = Field(default_factory=list)
	warnings: list[str] = Field(default_factory=list)

	dry_run_generated: Optional[DryRunResult] = None
	error: Optional[str] = None
