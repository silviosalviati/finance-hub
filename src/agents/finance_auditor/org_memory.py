"""Memória organizacional do Finance Voice IA — fatos persistentes por usuário.

Persistência em SQLite (`finance_org_facts`). Busca lexical (token overlap)
sobre fact_text + tags. Sem vector DB — Fase 6 trocará por embeddings.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from src.core.database import (
    delete_org_fact,
    insert_org_fact,
    list_org_facts,
)

_TOKEN_RE = re.compile(r"[a-zA-ZÀ-ÿ0-9_]+")


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


def save_fact(
    user_id: str,
    fact_text: str,
    tags: str = "",
    scope: str = "user",
) -> int | None:
    fact_text = (fact_text or "").strip()
    if not fact_text:
        return None
    return insert_org_fact(
        user_id=str(user_id or ""),
        fact_text=fact_text[:1000],
        tags=str(tags or "")[:200],
        scope=(scope or "user").strip().lower(),
    )


def recall(
    user_id: str,
    query: str,
    top_k: int = 5,
    include_global: bool = True,
) -> list[dict[str, Any]]:
    q_tokens = _tokens(query)
    facts = list_org_facts(user_id=user_id, include_global=include_global, limit=200)
    if not q_tokens:
        return facts[:top_k]
    scored: list[tuple[int, dict[str, Any]]] = []
    for f in facts:
        text = f"{f.get('fact_text', '')} {f.get('tags', '')}"
        score = len(q_tokens.intersection(_tokens(text)))
        if score > 0:
            scored.append((score, f))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [f for _, f in scored[:top_k]]


def forget(fact_id: int) -> bool:
    return delete_org_fact(int(fact_id))


__all__ = ["save_fact", "recall", "forget"]
