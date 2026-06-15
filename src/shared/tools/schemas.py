from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator


class QueryAntiPattern(BaseModel):
    pattern: str
    description: str
    severity: str
    suggestion: str

    @field_validator("severity")
    @classmethod
    def normalize_severity(cls, v: str) -> str:
        normalized = (v or "").strip().upper()
        return normalized if normalized in {"LOW", "MEDIUM", "HIGH", "CRITICAL"} else "MEDIUM"


class AntipatternList(BaseModel):
    antipatterns: list[QueryAntiPattern] = Field(default_factory=list)


class IntelligenceReport(BaseModel):
    """Structured output de enrich_with_intelligence."""
    table_alternatives: list[str] = Field(default_factory=list)
    partition_opportunities: list[str] = Field(default_factory=list)
    clustering_opportunities: list[str] = Field(default_factory=list)
    dataset_insights: list[str] = Field(default_factory=list)
    summary: str = ""


class DryRunResult(BaseModel):
    bytes_processed: int
    bytes_billed: int
    estimated_cost_usd: float
    slot_ms_estimate: Optional[int] = None
    referenced_tables: list[str] = Field(default_factory=list)
    error: Optional[str] = None

    @property
    def gb_processed(self) -> float:
        return self.bytes_processed / (1024**3)

    @property
    def tb_processed(self) -> float:
        return self.bytes_processed / (1024**4)


class DateRange(BaseModel):
    date_start: str
    date_end: str


class ThemeItem(BaseModel):
    nome: str
    frequencia_estimada: int
    sentimento_predominante: str

    @field_validator("sentimento_predominante")
    @classmethod
    def normalize_sentiment(cls, v: str) -> str:
        normalized = (v or "").strip().upper()
        return normalized if normalized in {"POSITIVO", "NEGATIVO", "NEUTRO"} else "NEUTRO"


class ThemesResponse(BaseModel):
    themes: list[ThemeItem] = Field(default_factory=list)
    insights: str = ""


class ReportResponse(BaseModel):
    markdown_report: str
    quality_score: int


class SuggestionsResponse(BaseModel):
    suggestions: list[str] = Field(default_factory=list)


class OptimizationReport(BaseModel):
    efficiency_score: int
    grade: str
    summary: str
    antipatterns_found: list[QueryAntiPattern] = Field(default_factory=list)
    optimized_query: Optional[str] = None
    original_query: Optional[str] = None
    bytes_saved: Optional[int] = None
    cost_saved_usd: Optional[float] = None
    savings_pct: Optional[float] = None
    recommendations: list[str] = Field(default_factory=list)
    power_bi_tips: list[str] = Field(default_factory=list)
    applied_optimizations: list[str] = Field(default_factory=list)
    intelligence_summary: Optional[str] = None
    data_existence_warning: Optional[str] = None
    optimization_status: str = "skipped_no_antipatterns"  # "approved" | "skipped_by_user" | "skipped_no_antipatterns" | "failed"
    data_quality: str = "full"  # "full" | "partial" | "no_cost_data"
    optimization_impact: str = "none"  # "bytes_and_slots" | "slots_only" | "none"

