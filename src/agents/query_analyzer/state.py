from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from src.shared.tools.schemas import DryRunResult, OptimizationReport, QueryAntiPattern


class AgentState(BaseModel):
    original_query: str
    project_id: str
    dataset_hint: Optional[str] = None

    query_structure: dict = Field(default_factory=dict)

    dry_run_original: Optional[DryRunResult] = None

    antipatterns: list[QueryAntiPattern] = Field(default_factory=list)
    needs_optimization: bool = False
    optimization_feedback: list[str] = Field(default_factory=list)

    optimized_query: Optional[str] = None
    dry_run_optimized: Optional[DryRunResult] = None

    report: Optional[OptimizationReport] = None

    error: Optional[str] = None
    iteration: int = 0
    max_iterations: int = 5
