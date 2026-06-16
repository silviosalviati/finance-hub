"""Forecast simples — regressão linear OLS pura Python.

Detecta tendência linear sobre uma série (x = índice ordinal, y = valor) e
projeta `horizon` períodos à frente. Sem dependências externas — para
modelos sazonais/ARIMA migrar para Prophet em fase futura.
"""

from __future__ import annotations

from typing import Any


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return None
    return None


def linear_regression(values: list[float]) -> dict[str, float]:
    """Retorna slope, intercept e r² (coeficiente de determinação)."""
    n = len(values)
    if n < 2:
        return {"slope": 0.0, "intercept": float(values[0]) if values else 0.0, "r2": 0.0}
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    num = sum((xs[i] - mean_x) * (values[i] - mean_y) for i in range(n))
    den = sum((xs[i] - mean_x) ** 2 for i in range(n)) or 1e-12
    slope = num / den
    intercept = mean_y - slope * mean_x
    ss_tot = sum((y - mean_y) ** 2 for y in values) or 1e-12
    ss_res = sum((values[i] - (slope * xs[i] + intercept)) ** 2 for i in range(n))
    r2 = max(0.0, 1.0 - ss_res / ss_tot)
    return {"slope": slope, "intercept": intercept, "r2": r2}


def project(
    rows: list[dict[str, Any]],
    value_column: str,
    horizon: int = 6,
    time_column: str | None = None,
) -> dict[str, Any]:
    """Devolve resumo da regressão + projeções para `horizon` períodos.

    Quando `time_column` é informado, ordena por ele (string ou número).
    Quando ausente, mantém a ordem das rows.
    """
    if not rows:
        return {"ok": False, "error": "Sem linhas para prever."}
    if value_column not in rows[0]:
        return {"ok": False, "error": f"Coluna '{value_column}' não encontrada."}

    ordered = list(rows)
    if time_column and time_column in rows[0]:
        ordered.sort(key=lambda r: str(r.get(time_column) or ""))

    series: list[float] = []
    skipped = 0
    for r in ordered:
        v = _to_float(r.get(value_column))
        if v is None:
            skipped += 1
            continue
        series.append(v)
    if len(series) < 2:
        return {"ok": False, "error": "Série numérica com menos de 2 pontos."}

    reg = linear_regression(series)
    horizon = max(1, min(int(horizon or 6), 60))
    n = len(series)
    forecasts = [
        {
            "step": i + 1,
            "x": n + i,
            "y": round(reg["slope"] * (n + i) + reg["intercept"], 6),
        }
        for i in range(horizon)
    ]
    direction = "alta" if reg["slope"] > 0 else ("baixa" if reg["slope"] < 0 else "estavel")
    return {
        "ok": True,
        "value_column": value_column,
        "time_column": time_column,
        "n_points": n,
        "skipped": skipped,
        "slope": round(reg["slope"], 6),
        "intercept": round(reg["intercept"], 6),
        "r2": round(reg["r2"], 4),
        "direction": direction,
        "forecasts": forecasts,
    }


__all__ = ["linear_regression", "project"]
