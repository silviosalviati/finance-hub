"""Semantic Layer do Finance Voice IA.

As metricas sao definidas por administradores em runtime (CRUD via API) e
persistidas em `finance_semantic_metrics` (SQLite). O lookup continua sendo
lexical, mas com normalizacao e ranking mais estavel para melhorar a
descoberta das metricas registradas.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date, timedelta
from typing import Any

from src.core.database import get_finance_metric, list_finance_metrics

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
_PLACEHOLDER_PARAMS: tuple[str, ...] = ("date_start", "date_end", "limit")
_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _strip_accents(text: str) -> str:
    return "".join(
        c
        for c in unicodedata.normalize("NFD", text or "")
        if unicodedata.category(c) != "Mn"
    )


def _slug(text: str) -> str:
    base = _strip_accents(text).lower()
    base = re.sub(r"[^a-z0-9]+", "_", base)
    return base.strip("_")


def _normalize_token(token: str) -> str:
    token = _strip_accents(token).lower().strip("_")
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 4 and token.endswith("es"):
        return token[:-2]
    if len(token) > 3 and token.endswith("s"):
        return token[:-1]
    return token


def _tokens(text: str) -> set[str]:
    normalized = _strip_accents(text).lower().replace("_", " ")
    return {
        _normalize_token(token)
        for token in _TOKEN_RE.findall(normalized)
        if len(token) > 2
    }


def _normalize_date(value: Any, fallback: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    if not _ISO_DATE_RE.fullmatch(raw):
        return fallback
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return fallback


def _normalize_limit(value: Any, default: int = 200) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(limit, 1000))


def _metric_score(metric: dict[str, Any], query: str, q_tokens: set[str]) -> int:
    fields = (
        ("key", 5),
        ("name", 4),
        ("tags", 3),
        ("description", 2),
    )
    score = 0
    for field, weight in fields:
        score += len(q_tokens.intersection(_tokens(str(metric.get(field, ""))))) * weight

    normalized_query = _slug(query).replace("_", " ").strip()
    if normalized_query:
        for field, bonus in (("name", 4), ("description", 2), ("tags", 2)):
            haystack = _slug(str(metric.get(field, ""))).replace("_", " ")
            if normalized_query in haystack:
                score += bonus
    return score


def search_metrics(query: str, top_k: int = 5) -> list[dict[str, Any]]:
    """Retorna metricas ordenadas por relevancia lexical."""
    q_tokens = _tokens(query)
    if not q_tokens:
        return []

    try:
        limit = int(top_k)
    except (TypeError, ValueError):
        limit = 5
    limit = max(1, min(limit, 20))

    ranked: list[tuple[int, str, dict[str, Any]]] = []
    for metric in list_finance_metrics():
        score = _metric_score(metric, query, q_tokens)
        if score > 0:
            ranked.append((score, str(metric.get("key") or ""), metric))

    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [metric for _, _, metric in ranked[:limit]]


def default_period() -> tuple[str, str]:
    today = date.today()
    return (today - timedelta(days=30)).isoformat(), today.isoformat()


def render_sql(
    sql_template: str,
    params: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Substitui placeholders aceitos com defaults estaveis."""
    if not sql_template:
        return "", {}

    provided = dict(params or {})
    default_start, default_end = default_period()
    date_start = _normalize_date(provided.get("date_start"), default_start)
    date_end = _normalize_date(provided.get("date_end"), default_end)
    if date_start > date_end:
        date_start, date_end = date_end, date_start

    resolved: dict[str, Any] = {
        "date_start": date_start,
        "date_end": date_end,
        "limit": _normalize_limit(provided.get("limit"), default=200),
    }

    used: dict[str, Any] = {}

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in resolved:
            return match.group(0)
        used[key] = resolved[key]
        return str(resolved[key])

    rendered = _PLACEHOLDER_RE.sub(replace, sql_template)
    return rendered, used


def resolve_metric(key: str) -> dict[str, Any] | None:
    raw_key = str(key or "").strip()
    if not raw_key:
        return None

    metric = get_finance_metric(raw_key)
    if metric:
        return metric

    normalized_key = _slug(raw_key)
    if not normalized_key:
        return None

    for candidate in list_finance_metrics():
        candidate_key = _slug(str(candidate.get("key") or ""))
        candidate_name = _slug(str(candidate.get("name") or ""))
        if normalized_key in {candidate_key, candidate_name}:
            return candidate
    return None


__all__ = [
    "default_period",
    "render_sql",
    "resolve_metric",
    "search_metrics",
]
