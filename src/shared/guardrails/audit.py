"""Audit trail do Finance Voice IA.

Resume cada execução do Supervisor (request, persona, plano, custos BQ
acumulados, erro) e persiste em `finance_audit_log` via core.database.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.core.database import append_finance_audit


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def summarize_costs(tool_results: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Soma bytes_processed e estimated_cost_usd vindos das capabilities BQ."""
    bytes_total = 0
    cost_total = 0.0
    for r in tool_results or []:
        payload = r.get("payload") or {}
        if not isinstance(payload, dict):
            continue
        bp = payload.get("bytes_processed")
        cu = payload.get("estimated_cost_usd")
        if isinstance(bp, (int, float)):
            bytes_total += int(bp)
        if isinstance(cu, (int, float)):
            cost_total += float(cu)
    return {"bytes_processed": bytes_total, "estimated_cost_usd": round(cost_total, 6)}


def record(state: dict[str, Any]) -> int | None:
    """Persiste uma entrada de auditoria a partir do estado final do Supervisor."""
    try:
        tool_results = state.get("tool_results") or []
        steps_ok = sum(1 for r in tool_results if r and r.get("ok"))
        plan = state.get("plan") or []
        costs = summarize_costs(tool_results)
        entry = {
            "ts": _utcnow_iso(),
            "user_id": str(state.get("user_id") or ""),
            "persona": str(state.get("persona") or ""),
            "request_text": str(state.get("request_text") or ""),
            "plan": plan,
            "steps_total": len(tool_results),
            "steps_ok": steps_ok,
            "bytes_processed": costs["bytes_processed"],
            "estimated_cost_usd": costs["estimated_cost_usd"],
            "error": str(state.get("error") or ""),
        }
        return append_finance_audit(entry)
    except Exception:  # noqa: BLE001
        # Auditoria nunca derruba o fluxo principal.
        return None


__all__ = ["record", "summarize_costs"]
