"""Bloqueio de SQL destrutivo — único guardrail entre "o que a LLM gerou" e
"o que de fato roda no BigQuery". As funções de baixo nível em
`src.shared.tools.bigquery` (dry_run_query, fetch_query_sample,
execute_query_rows) não fazem nenhuma checagem própria — rodam o que vier.
Qualquer agente que execute SQL gerada por LLM contra o BigQuery deve
chamar `assert_select_only` antes.
"""

from __future__ import annotations

import re

_FORBIDDEN_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|TRUNCATE|CREATE|ALTER|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
_SELECT_OR_WITH_PATTERN = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def assert_select_only(sql: str) -> str | None:
    """Retorna uma mensagem de erro se `sql` não for uma leitura segura
    (SELECT/WITH, sem DDL/DML embutido) — ou `None` se estiver liberada.
    """
    text = (sql or "").strip()
    if not text:
        return "sql vazio."
    if _FORBIDDEN_PATTERN.search(text):
        return "Apenas queries de leitura (SELECT/WITH) são permitidas."
    if not _SELECT_OR_WITH_PATTERN.match(text):
        return "Apenas queries iniciando com SELECT ou WITH são permitidas."
    return None


__all__ = ["assert_select_only"]
