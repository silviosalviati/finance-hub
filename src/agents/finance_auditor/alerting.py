"""Alerting do Finance Voice IA — executa métricas com threshold.

Cada métrica do Semantic Layer pode opcionalmente ter `alert_threshold` (JSON):

    {
      "column": "valor_total",      # coluna inspecionada
      "op": ">=" | "<=" | ">" | "<" | "==",
      "value": 1000.0,              # número de referência
      "aggregate": "max|min|sum|avg" # opcional, default = primeiro valor
    }

`run_alerts()` percorre métricas com threshold configurado, executa o SQL,
compara conforme o operador e devolve a lista de "briefings" disparados.
Não dispara notificações por si só — é trigger-driven (chamável por um cron
externo no endpoint /admin/finance/alerts/run).
"""

from __future__ import annotations

import json
from typing import Any

from src.agents.finance_auditor import semantic_layer
from src.agents.finance_auditor.capabilities import _validate_and_run_sql
from src.core.database import list_finance_metrics

_OPS = {">", ">=", "<", "<=", "==", "!="}
_AGGREGATES = {"max", "min", "sum", "avg", "first"}


def _aggregate(rows: list[dict[str, Any]], column: str, kind: str) -> float | None:
    vals: list[float] = []
    for r in rows:
        v = r.get(column)
        if v is None:
            continue
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            continue
    if not vals:
        return None
    if kind == "max":
        return max(vals)
    if kind == "min":
        return min(vals)
    if kind == "sum":
        return sum(vals)
    if kind == "avg":
        return sum(vals) / len(vals)
    return vals[0]


def _compare(left: float, op: str, right: float) -> bool:
    return {
        ">": lambda: left > right,
        ">=": lambda: left >= right,
        "<": lambda: left < right,
        "<=": lambda: left <= right,
        "==": lambda: left == right,
        "!=": lambda: left != right,
    }[op]()


def _parse_threshold(raw: str) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("op") not in _OPS:
        return None
    if "value" not in data or "column" not in data:
        return None
    agg = (data.get("aggregate") or "first").lower()
    if agg not in _AGGREGATES:
        agg = "first"
    data["aggregate"] = agg
    return data


def run_alerts(
    project_id: str,
    user: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for metric in list_finance_metrics():
        threshold_raw = metric.get("alert_threshold") or ""
        threshold = _parse_threshold(threshold_raw)
        if not threshold:
            continue
        sql, params_used = semantic_layer.render_sql(
            metric.get("sql_template", ""), params or {}
        )
        if not sql:
            continue
        exec_result = _validate_and_run_sql(
            sql=sql,
            project_id=project_id,
            max_rows=int((params or {}).get("limit") or 200),
            user=user,
        )
        entry: dict[str, Any] = {
            "metric_key": metric.get("key"),
            "metric_name": metric.get("name"),
            "ok": exec_result.get("ok"),
            "params_used": params_used,
            "threshold": threshold,
        }
        if not exec_result.get("ok"):
            entry["error"] = exec_result.get("error")
            out.append(entry)
            continue
        rows = (exec_result.get("payload") or {}).get("rows") or []
        actual = _aggregate(rows, threshold["column"], threshold["aggregate"])
        entry["actual"] = actual
        entry["triggered"] = False
        if actual is not None:
            try:
                entry["triggered"] = _compare(float(actual), threshold["op"], float(threshold["value"]))
            except (TypeError, ValueError):
                entry["triggered"] = False
        out.append(entry)
    return out


__all__ = ["run_alerts"]
