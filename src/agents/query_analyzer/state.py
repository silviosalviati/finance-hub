from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from src.shared.tools.schemas import DryRunResult, OptimizationReport, QueryAntiPattern


class AgentState(BaseModel):
    original_query: str
    project_id: str
    dataset_hint: Optional[str] = None

    query_structure: dict = Field(default_factory=dict)

    # fan-out: discover_context (3 workers paralelos)
    query_schema: str = ""          # schema das tabelas referenciadas na query
    dataset_catalog: str = ""       # catálogo completo de todas as tabelas do dataset
    schema_context: str = ""        # contexto enriquecido (schema + intelligence) — usado em optimize_query

    dry_run_original: Optional[DryRunResult] = None

    intelligence_context: str = ""  # análise LLM: alternativas, partições, oportunidades

    antipatterns: list[QueryAntiPattern] = Field(default_factory=list)
    needs_optimization: bool = False
    optimization_feedback: list[str] = Field(default_factory=list)

    optimized_query: Optional[str] = None
    dry_run_optimized: Optional[DryRunResult] = None

    report: Optional[OptimizationReport] = None

    error: Optional[str] = None
    iteration: int = 0
    max_iterations: int = 2
    human_decision: Optional[str] = None  # "approve" | "skip" | texto livre
