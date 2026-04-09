from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class QueryAntiPattern(BaseModel):
    pattern: str
    description: str
    severity: str
    line_hint: Optional[str] = None
    suggestion: str


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


class OptimizationReport(BaseModel):
    efficiency_score: int
    grade: str
    summary: str
    antipatterns_found: list[QueryAntiPattern] = Field(default_factory=list)
    optimized_query: Optional[str] = None
    bytes_saved: Optional[int] = None
    cost_saved_usd: Optional[float] = None
    savings_pct: Optional[float] = None
    recommendations: list[str] = Field(default_factory=list)
    power_bi_tips: list[str] = Field(default_factory=list)
