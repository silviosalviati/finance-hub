"""Semantic Layer do Finance Voice IA — sem métricas pré-cadastradas.

Métricas são definidas por administradores em runtime (CRUD via API) e
persistidas em `finance_semantic_metrics` (SQLite). Cada métrica tem:

    key            — identificador único (slug).
    name           — nome humano para o LLM/usuário.
    description    — texto curto usado no lookup lexical.
    source_table   — referência canônica (informativa).
    sql_template   — SQL parametrizável via placeholders {date_start}/{date_end}.
    owner / tags   — metadados de governança.

Lookup é puramente lexical (token overlap) — não exige embeddings.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date, timedelta
from typing import Any

from src.core.database import (
    get_finance_metric,
    list_finance_metrics,
)

_TOKEN_RE = re.compile(r"[a-zA-ZÀ-ÿ0-9_]+")
_PLACEHOLDER_PARAMS: tuple[str, ...] = ("date_start", "date_end", "limit")


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text or "")
        if unicodedata.category(c) != "Mn"
    )


def _tokens(text: str) -> set[str]:
    return {
        t.lower() for t in _TOKEN_RE.findall(_strip_accents(text or ""))
        if len(t) > 2
    }


def search_metrics(query: str, top_k: int = 5) -> list[dict[str, Any]]:
    """Retorna métricas ordenadas por sobreposição lexical com a query."""
    q_tokens = _tokens(query)
    if not q_tokens:
        return []
    out: list[tuple[int, dict[str, Any]]] = []
    for m in list_finance_metrics():
        haystack = " ".join(
            [
                m.get("key", ""),
                m.get("name", ""),
                m.get("description", ""),
                m.get("tags", ""),
            ]
        )
        score = len(q_tokens.intersection(_tokens(haystack)))
        if score > 0:
            out.append((score, m))
    out.sort(key=lambda item: item[0], reverse=True)
    return [m for _, m in out[:top_k]]


def default_period() -> tuple[str, str]:
    today = date.today()
    return (today - timedelta(days=30)).isoformat(), today.isoformat()


def render_sql(sql_template: str, params: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
    """Substitui placeholders aceitos. Defaults: últimos 30 dias e limit=200."""
    if not sql_template:
        return "", {}
    p = dict(params or {})
    if "date_start" not in p or "date_end" not in p:
        ds, de = default_period()
        p.setdefault("date_start", ds)
        p.setdefault("date_end", de)
    p.setdefault("limit", 200)

    # Só substitui placeholders conhecidos — evita interpolação acidental.
    rendered = sql_template
    used: dict[str, Any] = {}
    for key in _PLACEHOLDER_PARAMS:
        token = "{" + key + "}"
        if token in rendered:
            value = p.get(key)
            rendered = rendered.replace(token, str(value))
            used[key] = value
    return rendered, used


def resolve_metric(key: str) -> dict[str, Any] | None:
    return get_finance_metric(key)


__all__ = [
    "search_metrics",
    "render_sql",
    "resolve_metric",
    "default_period",
]
