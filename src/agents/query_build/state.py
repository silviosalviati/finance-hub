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

	# Sessão do usuário (RBAC + auditoria) — sem isso check_access/record_audit
	# não têm como saber quem está pedindo.
	user: dict[str, Any] = Field(default_factory=dict)

	generated_sql: Optional[str] = None
	explanation: str = ""
	assumptions: list[str] = Field(default_factory=list)
	warnings: list[str] = Field(default_factory=list)

	# Contador de autocorreção por erro DURO (coluna/tabela inexistente, SQL
	# que não passa no dry-run) — limite de 1, separado do contador de
	# qualidade abaixo (cada um tem seu próprio teto no grafo).
	repair_attempts: int = 0
	# True quando o `error` atual é o tipo que regenerar a SQL pode corrigir
	# (schema/sintaxe) — False para bloqueio de RBAC ou SQL insegura, que
	# voltar pro generate_sql não resolveria.
	repairable_error: bool = False

	dry_run_generated: Optional[DryRunResult] = None

	# Score de boas práticas (0-100) contra os 5 PILARES OBRIGATÓRIOS do
	# QUERY_BUILD_SYSTEM_PROMPT — e o HITL que decide o que fazer quando < 80.
	quality_score: int = 0
	quality_issues: list[str] = Field(default_factory=list)
	quality_retry_count: int = 0
	human_decision: Optional[str] = None

	sample_columns: list[str] = Field(default_factory=list)
	sample_rows: list[dict[str, Any]] = Field(default_factory=list)
	sample_error: Optional[str] = None
	error: Optional[str] = None
	# Categoria do erro ("rbac" | "schema" | "sql_safety" | "bigquery_syntax" |
	# "budget" | "llm_api") — usada por _friendlify_error() pra traduzir o
	# erro técnico em mensagem simples sem depender de casar texto.
	error_category: str = ""
