from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from src.shared.tools.schemas import DryRunResult


class QueryTransformerState(BaseModel):
    request_sql: str
    project_id: str

    # Sessão do usuário (RBAC + auditoria) — mesmo papel de `user` em QueryBuildState.
    user: dict[str, Any] = Field(default_factory=dict)

    sql_kind: str = ""
    statement_count: int = 0
    dependencies: list[dict[str, str]] = Field(default_factory=list)
    security_findings: list[dict[str, str]] = Field(default_factory=list)
    required_questions: list[dict[str, Any]] = Field(default_factory=list)
    user_answers: dict[str, Any] = Field(default_factory=dict)
    architecture_recommendation: str = ""
    architecture_confidence: float = 0.0
    requirements_confirmed: bool = False
    architecture_decision: str = ""
    baseline_analysis: dict[str, Any] = Field(default_factory=dict)
    cost_reduction_pct: float = 0.0

    dry_run_original: Optional[DryRunResult] = None

    config_block: str = ""
    query_body: str = ""
    materialization_type: str = ""
    suggested_refs: list[str] = Field(default_factory=list)
    rationale: str = ""
    sqlx_content: str = ""

    dry_run_generated: Optional[DryRunResult] = None
    equivalence_ok: bool = False
    equivalence_diff: str = ""

    # Contador de autocorreção por erro DURO (SQLX malformado, dry-run
    # falhou) — limite de 1, separado do contador de qualidade abaixo.
    repair_attempts: int = 0
    repairable_error: bool = False

    quality_score: int = 0
    quality_issues: list[str] = Field(default_factory=list)
    quality_retry_count: int = 0
    human_decision: Optional[str] = None

    warnings: list[str] = Field(default_factory=list)
    error: Optional[str] = None
    # Categoria do erro ("rbac" | "sql_not_select" | "bigquery_syntax" |
    # "llm_api") — mesma função de QueryBuildState.error_category.
    error_category: str = ""
