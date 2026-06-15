"""Catálogo de capabilities (tools) do Supervisor do Finance Voice IA.

Cada capability é uma função pura de assinatura:
    fn(args: dict, context: dict) -> dict

`context` carrega: project_id, dataset_hint, request_text, legacy_agent.
O retorno é serializável (JSON-safe) com chaves:
    {"ok": bool, "payload": <dict|list|str>, "error": <str|None>, "artifacts": [...]}

Os wrappers são finos — toda a lógica BigQuery vive em src/shared/tools/bigquery.py
e o pipeline VoC vive no grafo legado FinanceAuditor.
"""

from __future__ import annotations

import re
from typing import Any, Callable

from src.agents.finance_auditor.supervisor_schemas import (
    CAPABILITY_BQ_GET_SCHEMA,
    CAPABILITY_BQ_LIST_TABLES,
    CAPABILITY_BQ_QUERY,
    CAPABILITY_CHAT_ANSWER,
    CAPABILITY_VOC_REPORT,
)
from src.shared.config import get_runtime_config
from src.shared.tools.bigquery import (
    dry_run_query,
    execute_query_rows,
    get_dataset_tables_metadata,
    get_table_schema,
)

# Budget máximo, em bytes, escaneável por um único bq_query do Supervisor.
# Default 5 GiB — protege contra Text-to-SQL descontrolado.
_DEFAULT_BQ_QUERY_BUDGET_BYTES = 5 * 1024 ** 3
_DEFAULT_BQ_QUERY_MAX_ROWS = 200
_SQL_FORBIDDEN_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|TRUNCATE|CREATE|ALTER|GRANT|REVOKE)\b",
    re.IGNORECASE,
)


def _ok(payload: Any, artifacts: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"ok": True, "payload": payload, "error": None, "artifacts": artifacts or []}


def _err(message: str) -> dict[str, Any]:
    return {"ok": False, "payload": None, "error": message, "artifacts": []}


# ---------------------------------------------------------------------------
# voc_report — encapsula o pipeline legado VoC
# ---------------------------------------------------------------------------

def cap_voc_report(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    legacy_agent = context.get("legacy_agent")
    if legacy_agent is None:
        return _err("Pipeline VoC indisponível neste contexto.")
    try:
        result = legacy_agent.legacy_analyze(
            query=context.get("request_text", ""),
            project_id=context.get("project_id", ""),
            dataset_hint=context.get("dataset_hint"),
        )
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao executar pipeline VoC: {exc}")

    if result.get("status") != "ok":
        return _err(result.get("error") or "Pipeline VoC retornou erro.")

    return _ok(
        payload={
            "markdown_report": result.get("markdown_report", ""),
            "quality_score": result.get("quality_score", 0),
            "friction_score": result.get("friction_score", 0.0),
            "friction_label": result.get("friction_label", "N/A"),
            "total_records": result.get("total_records", 0),
            "operations_analyzed": result.get("operations_analyzed", []),
            "date_range": result.get("date_range", {}),
            "sentiment_analysis": result.get("sentiment_analysis", {}),
            "friction_analysis": result.get("friction_analysis", {}),
            "themes_analysis": result.get("themes_analysis", {}),
        },
        artifacts=[
            {
                "type": "voc_report",
                "markdown": result.get("markdown_report", ""),
                "metrics": {
                    "friction_score": result.get("friction_score", 0.0),
                    "friction_label": result.get("friction_label", "N/A"),
                    "total_records": result.get("total_records", 0),
                },
            }
        ],
    )


# ---------------------------------------------------------------------------
# bq_list_tables
# ---------------------------------------------------------------------------

def cap_bq_list_tables(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    project_id = context.get("project_id") or ""
    dataset_hint = (
        args.get("dataset_hint")
        or context.get("dataset_hint")
        or get_runtime_config("FINANCE_AUDITOR_DEFAULT_DATASET", "ds_inteligencia_analitica")
    )
    if not project_id:
        return _err("project_id ausente para listar tabelas.")
    try:
        info = get_dataset_tables_metadata(project_id, dataset_hint, max_tables=30, max_columns=15)
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao listar tabelas: {exc}")

    tables = info.get("tables", [])
    return _ok(
        payload={"dataset_ref": info.get("dataset_ref", ""), "tables": tables},
        artifacts=[
            {
                "type": "table",
                "title": f"Tabelas em {info.get('dataset_ref', '')}",
                "columns": ["table_id", "columns"],
                "rows": [
                    {"table_id": t.get("table_id", ""), "columns": ", ".join(t.get("columns", []) or [])}
                    for t in tables
                ],
            }
        ],
    )


# ---------------------------------------------------------------------------
# bq_get_schema
# ---------------------------------------------------------------------------

def cap_bq_get_schema(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    table_ref = str(args.get("table_ref") or "").strip()
    if not table_ref or table_ref.count(".") != 2:
        return _err("table_ref inválido. Use 'projeto.dataset.tabela'.")
    project_id = context.get("project_id") or table_ref.split(".", 1)[0]
    try:
        schema_text = get_table_schema(table_ref, project_id, max_columns=50)
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao obter schema: {exc}")

    return _ok(
        payload={"table_ref": table_ref, "schema": schema_text},
        artifacts=[{"type": "schema", "table_ref": table_ref, "text": schema_text}],
    )


# ---------------------------------------------------------------------------
# bq_query — com guardrails de segurança e budget
# ---------------------------------------------------------------------------

def cap_bq_query(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    sql = str(args.get("sql") or "").strip().rstrip(";")
    if not sql:
        return _err("sql vazio.")
    if _SQL_FORBIDDEN_PATTERN.search(sql):
        return _err("Apenas queries de leitura (SELECT/WITH) são permitidas.")
    if not re.match(r"^\s*(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
        return _err("Apenas queries iniciando com SELECT ou WITH são permitidas.")

    project_id = context.get("project_id") or ""
    if not project_id:
        return _err("project_id ausente para executar query.")

    max_rows = int(args.get("max_rows") or _DEFAULT_BQ_QUERY_MAX_ROWS)
    max_rows = max(1, min(max_rows, 1000))

    budget = int(
        get_runtime_config("FINANCE_AUDITOR_QUERY_BUDGET_BYTES", str(_DEFAULT_BQ_QUERY_BUDGET_BYTES))
    )

    # 1) dry-run obrigatório para estimar custo / bytes
    dry = dry_run_query(sql, project_id, timeout_seconds=20)
    if dry.error:
        return _err(f"Dry-run falhou: {dry.error}")
    if dry.bytes_processed > budget:
        gb = dry.bytes_processed / (1024 ** 3)
        gb_budget = budget / (1024 ** 3)
        return _err(
            f"Query excede o budget de {gb_budget:.2f} GiB "
            f"(estimado {gb:.2f} GiB). Refine os filtros."
        )

    # 2) execução com limite de linhas
    try:
        rows = execute_query_rows(sql, project_id, max_rows=max_rows)
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha na execução: {exc}")

    columns = list(rows[0].keys()) if rows else []
    return _ok(
        payload={
            "sql": sql,
            "row_count": len(rows),
            "bytes_processed": dry.bytes_processed,
            "estimated_cost_usd": dry.estimated_cost_usd,
            "rows": rows,
        },
        artifacts=[
            {"type": "sql", "sql": sql},
            {"type": "table", "title": "Resultado da query", "columns": columns, "rows": rows},
        ],
    )


# ---------------------------------------------------------------------------
# chat_answer — placeholder; resposta narrativa fica a cargo do Composer
# ---------------------------------------------------------------------------

def cap_chat_answer(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    return _ok(payload={"note": "Resposta conversacional sem consulta a dados."})


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

CAPABILITY_REGISTRY: dict[str, Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]] = {
    CAPABILITY_VOC_REPORT: cap_voc_report,
    CAPABILITY_BQ_LIST_TABLES: cap_bq_list_tables,
    CAPABILITY_BQ_GET_SCHEMA: cap_bq_get_schema,
    CAPABILITY_BQ_QUERY: cap_bq_query,
    CAPABILITY_CHAT_ANSWER: cap_chat_answer,
}


def execute_capability(capability: str, args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    fn = CAPABILITY_REGISTRY.get(capability)
    if fn is None:
        return _err(f"Capability desconhecida: {capability}")
    return fn(args or {}, context or {})


__all__ = [
    "CAPABILITY_REGISTRY",
    "execute_capability",
    "cap_voc_report",
    "cap_bq_list_tables",
    "cap_bq_get_schema",
    "cap_bq_query",
    "cap_chat_answer",
]
