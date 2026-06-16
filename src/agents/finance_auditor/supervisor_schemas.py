"""Schemas Pydantic do Supervisor (structured output do Planner)."""

from typing import Any

from pydantic import BaseModel, Field, field_validator

# Capabilities genéricas — sem nenhuma referência a domínio fixo.
CAPABILITY_BQ_LIST_DATASETS = "bq_list_datasets"
CAPABILITY_BQ_LIST_TABLES = "bq_list_tables"
CAPABILITY_BQ_GET_SCHEMA = "bq_get_schema"
CAPABILITY_BQ_QUERY = "bq_query"
CAPABILITY_TEXT_TO_SQL = "text_to_sql"
CAPABILITY_STATS_DESCRIBE = "stats_describe"
CAPABILITY_VIZ_SPEC = "viz_spec"
CAPABILITY_METRIC_LOOKUP = "metric_lookup"
CAPABILITY_METRIC_EXECUTE = "metric_execute"
CAPABILITY_CHAT_ANSWER = "chat_answer"

VALID_CAPABILITIES = {
    CAPABILITY_BQ_LIST_DATASETS,
    CAPABILITY_BQ_LIST_TABLES,
    CAPABILITY_BQ_GET_SCHEMA,
    CAPABILITY_BQ_QUERY,
    CAPABILITY_TEXT_TO_SQL,
    CAPABILITY_STATS_DESCRIBE,
    CAPABILITY_VIZ_SPEC,
    CAPABILITY_METRIC_LOOKUP,
    CAPABILITY_METRIC_EXECUTE,
    CAPABILITY_CHAT_ANSWER,
}


class PlanStep(BaseModel):
    """Um passo do plano produzido pelo Planner."""

    capability: str = Field(..., description="Nome da capability registrada.")
    args: dict[str, Any] = Field(default_factory=dict, description="Argumentos da capability.")
    rationale: str = Field(default="", description="Justificativa curta do uso desta capability.")

    @field_validator("capability")
    @classmethod
    def _validate_capability(cls, v: str) -> str:
        cap = (v or "").strip().lower()
        if not cap:
            return CAPABILITY_CHAT_ANSWER
        # Preserva o nome mesmo se inválido — o router responde com erro explícito.
        return cap


class PlanResponse(BaseModel):
    """Resposta do Planner: sequência de steps + justificativa global."""

    rationale: str = Field(default="", description="Resumo do plano.")
    steps: list[PlanStep] = Field(default_factory=list)


__all__ = [
    "CAPABILITY_BQ_LIST_DATASETS",
    "CAPABILITY_BQ_LIST_TABLES",
    "CAPABILITY_BQ_GET_SCHEMA",
    "CAPABILITY_BQ_QUERY",
    "CAPABILITY_TEXT_TO_SQL",
    "CAPABILITY_STATS_DESCRIBE",
    "CAPABILITY_VIZ_SPEC",
    "CAPABILITY_METRIC_LOOKUP",
    "CAPABILITY_METRIC_EXECUTE",
    "CAPABILITY_CHAT_ANSWER",
    "VALID_CAPABILITIES",
    "PlanStep",
    "PlanResponse",
]
