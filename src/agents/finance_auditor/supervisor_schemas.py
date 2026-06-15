"""Schemas Pydantic do Supervisor (structured output do Planner)."""

from typing import Any

from pydantic import BaseModel, Field, field_validator

CAPABILITY_VOC_REPORT = "voc_report"
CAPABILITY_BQ_LIST_TABLES = "bq_list_tables"
CAPABILITY_BQ_GET_SCHEMA = "bq_get_schema"
CAPABILITY_BQ_QUERY = "bq_query"
CAPABILITY_CHAT_ANSWER = "chat_answer"

VALID_CAPABILITIES = {
    CAPABILITY_VOC_REPORT,
    CAPABILITY_BQ_LIST_TABLES,
    CAPABILITY_BQ_GET_SCHEMA,
    CAPABILITY_BQ_QUERY,
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
        if cap not in VALID_CAPABILITIES:
            # mantém o valor para que o router decida fallback explícito,
            # em vez de levantar exceção dentro do structured output do LLM
            return cap or CAPABILITY_CHAT_ANSWER
        return cap


class PlanResponse(BaseModel):
    """Resposta do Planner: sequência de steps + justificativa global."""

    rationale: str = Field(default="", description="Resumo do plano.")
    steps: list[PlanStep] = Field(default_factory=list)


__all__ = [
    "CAPABILITY_VOC_REPORT",
    "CAPABILITY_BQ_LIST_TABLES",
    "CAPABILITY_BQ_GET_SCHEMA",
    "CAPABILITY_BQ_QUERY",
    "CAPABILITY_CHAT_ANSWER",
    "VALID_CAPABILITIES",
    "PlanStep",
    "PlanResponse",
]
