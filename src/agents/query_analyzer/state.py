from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from src.shared.tools.schemas import (
    DryRunResult,
    IntelligenceReport,
    OptimizationReport,
    QueryAntiPattern,
)


class AgentState(BaseModel):
    original_query: str
    project_id: str
    dataset_hint: Optional[str] = None

    query_structure: dict = Field(default_factory=dict)

    query_schema: str = ""          # schema das tabelas referenciadas na query
    dataset_catalog: str = ""       # catálogo completo de todas as tabelas do dataset
    dataset_memory: str = ""        # memória cross-sessão de padrões do dataset
    optimization_status: str = "pending"   # rastreado durante execução do pipeline

    dry_run_original: Optional[DryRunResult] = None

    intelligence_report: Optional[IntelligenceReport] = None  # saída estruturada do enrich

    data_existence_warning: Optional[str] = None  # aviso quando query otimizada não retorna dados

    antipatterns: list[QueryAntiPattern] = Field(default_factory=list)
    needs_optimization: bool = False
    optimization_feedback: list[str] = Field(default_factory=list)

    optimized_query: Optional[str] = None
    dry_run_optimized: Optional[DryRunResult] = None

    report: Optional[OptimizationReport] = None

    error: Optional[str] = None
    iteration: int = 0
    max_iterations: int = 2
    human_decision: Optional[str] = None  # "approve" | "skip"
