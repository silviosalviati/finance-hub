from __future__ import annotations

from typing import Any, TypedDict

TABLE_REF = "silviosalviati.ds_inteligencia_analitica.analitica_analise_ia"
DEFAULT_PROJECT = "silviosalviati"


class FinanceAuditorState(TypedDict, total=False):
    """Estado compartilhado do grafo FinanceAuditor (VoC + Fricção).

    Campos marcados como total=False são opcionais, permitindo que os nós
    paralelos atualizem chaves distintas sem conflito de escrita.
    """

    # --- Entrada ---
    request_text: str
    project_id: str
    dataset_hint: str | None

    # --- Saída do nó fetch_data ---
    generated_sql: str
    date_filter_start: str
    date_filter_end: str
    total_records: int
    raw_rows: list[dict[str, Any]]
    operations_analyzed: list[str]

    # --- Saídas dos nós paralelos (fan-out) ---
    sentiment_result: dict[str, Any]   # node_sentiment
    friction_result: dict[str, Any]    # node_friction
    themes_result: dict[str, Any]      # node_themes

    # --- Saída da consolidação ---
    friction_score: float
    friction_label: str
    consolidated_metrics: dict[str, Any]

    # --- Saída final ---
    markdown_report: str
    quality_score: int

    # --- Controle ---
    error: str | None
    warnings: list[str]
