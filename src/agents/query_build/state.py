from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from src.shared.tools.schemas import DryRunResult


class QueryBuildState(BaseModel):
	request_text: str
	project_id: str
	dataset_hint: Optional[str] = None
	dataset_tables: list[str] = Field(default_factory=list)
	dataset_table_columns: dict[str, list[str]] = Field(default_factory=dict)
	dataset_tables_context: str = ""

	generated_sql: Optional[str] = None
	explanation: str = ""
	assumptions: list[str] = Field(default_factory=list)
	warnings: list[str] = Field(default_factory=list)

	dry_run_generated: Optional[DryRunResult] = None
	sample_columns: list[str] = Field(default_factory=list)
	sample_rows: list[dict[str, Any]] = Field(default_factory=list)
	sample_error: Optional[str] = None
	error: Optional[str] = None
