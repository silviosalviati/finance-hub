"""PII Guard genérico — sem domínio fixo.

Detecta CPF, CNPJ, e-mail, telefone BR e cartão de crédito (padrões genéricos)
e aplica máscara, bloqueio ou passthrough conforme runtime config:

    FINANCE_AUDITOR_PII_MODE = "mask" (default) | "block" | "off"

Não substitui um DLP corporativo, mas oferece uma barreira de saída
configurável sobre `final_answer` e sobre as linhas dos artefatos.
"""

from __future__ import annotations

import re
from typing import Any

from src.shared.config import get_runtime_config

MODE_MASK = "mask"
MODE_BLOCK = "block"
MODE_OFF = "off"

_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    # CPF (com ou sem máscara) — três dígitos . três . três - dois
    ("cpf", re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b")),
    # CNPJ
    ("cnpj", re.compile(r"\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b")),
    # E-mail
    ("email", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
    # Telefone BR (com ou sem DDD/+55)
    (
        "phone",
        re.compile(
            r"(?:(?:\+?55\s*)?\(?\d{2}\)?[\s.-]?)?(?:9\s*)?\d{4}[\s.-]?\d{4}\b"
        ),
    ),
    # Cartão de crédito (13 a 19 dígitos com separadores opcionais)
    (
        "credit_card",
        re.compile(r"\b(?:\d[\s-]?){13,19}\b"),
    ),
)


def _resolve_mode() -> str:
    mode = (get_runtime_config("FINANCE_AUDITOR_PII_MODE", MODE_MASK) or MODE_MASK).strip().lower()
    return mode if mode in {MODE_MASK, MODE_BLOCK, MODE_OFF} else MODE_MASK


def _mask_match(kind: str, match: re.Match[str]) -> str:
    raw = match.group(0)
    digits = re.sub(r"\D", "", raw)
    if kind == "email":
        return "[email_REDACTED]"
    if kind in {"cpf", "cnpj", "credit_card"} and len(digits) >= 4:
        return f"[{kind.upper()}_***{digits[-4:]}]"
    if kind == "phone" and len(digits) >= 4:
        return f"[PHONE_***{digits[-4:]}]"
    return f"[{kind.upper()}_REDACTED]"


def scan(text: str) -> dict[str, int]:
    """Conta ocorrências por tipo (não muta o texto)."""
    if not text:
        return {}
    counts: dict[str, int] = {}
    for kind, pat in _PATTERNS:
        n = len(pat.findall(text))
        if n:
            counts[kind] = n
    return counts


def scrub_text(text: str) -> tuple[str, dict[str, int]]:
    """Substitui ocorrências por marcadores; devolve (texto_limpo, contagem)."""
    if not text:
        return text, {}
    counts: dict[str, int] = {}
    out = text
    for kind, pat in _PATTERNS:
        def _repl(m: re.Match[str], _kind: str = kind) -> str:
            counts[_kind] = counts.get(_kind, 0) + 1
            return _mask_match(_kind, m)

        out = pat.sub(_repl, out)
    return out, counts


def scrub_value(value: Any) -> Any:
    if isinstance(value, str):
        out, _ = scrub_text(value)
        return out
    if isinstance(value, dict):
        return {k: scrub_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [scrub_value(v) for v in value]
    return value


def _artifact_chart_values(art: dict[str, Any]) -> list[Any]:
    """Linhas embutidas em artefatos vega_lite (`spec.data.values`) — cap_viz_spec
    coloca os dados reais aí, não em `rows`, então o guard precisa olhar também."""
    if art.get("type") != "vega_lite":
        return []
    values = (((art.get("spec") or {}).get("data") or {}).get("values"))
    return values if isinstance(values, list) else []


def _artifact_stats_top_values(art: dict[str, Any]) -> list[Any]:
    """Valores mais frequentes por coluna em artefatos `stats` (cap_stats_describe) —
    `columns[col]["top"][i]["value"]` pode ser o valor bruto de uma coluna categórica
    (ex.: nome/e-mail), fora do alcance de `rows`/`sql`/`text`."""
    if art.get("type") != "stats":
        return []
    out: list[Any] = []
    for col_stats in (art.get("columns") or {}).values():
        for entry in (col_stats or {}).get("top") or []:
            out.append(entry.get("value"))
    return out


def apply_guard(
    final_answer: str,
    artifacts: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Aplica o guard conforme o modo configurado.

    Devolve sempre o mesmo shape:
        {
            "mode": <str>,
            "final_answer": <str>,
            "artifacts": <list>,
            "pii_counts": <dict>,
            "blocked": <bool>,
        }
    """
    mode = _resolve_mode()
    artifacts = artifacts or []

    if mode == MODE_OFF:
        return {
            "mode": mode,
            "final_answer": final_answer,
            "artifacts": artifacts,
            "pii_counts": {},
            "blocked": False,
        }

    counts_total: dict[str, int] = {}

    def _merge(extra: dict[str, int]) -> None:
        for k, v in extra.items():
            counts_total[k] = counts_total.get(k, 0) + v

    if mode == MODE_BLOCK:
        # Apenas detecta — se houver PII, devolve mensagem genérica.
        _merge(scan(final_answer or ""))
        for art in artifacts:
            for v in (art.get("rows") or []):
                _merge(scan(str(v)))
            _merge(scan(str(art.get("sql") or "")))
            _merge(scan(str(art.get("text") or "")))
            for v in _artifact_chart_values(art):
                _merge(scan(str(v)))
            for v in _artifact_stats_top_values(art):
                _merge(scan(str(v)))
        if counts_total:
            return {
                "mode": mode,
                "final_answer": (
                    "_Resposta bloqueada pelo PII Guard — foram detectados "
                    f"dados sensíveis: {', '.join(sorted(counts_total))}._"
                ),
                "artifacts": [],
                "pii_counts": counts_total,
                "blocked": True,
            }
        return {
            "mode": mode,
            "final_answer": final_answer,
            "artifacts": artifacts,
            "pii_counts": {},
            "blocked": False,
        }

    # MODE_MASK (default)
    scrubbed_answer, c1 = scrub_text(final_answer or "")
    _merge(c1)
    scrubbed_artifacts: list[dict[str, Any]] = []
    for art in artifacts:
        new = dict(art)
        if "rows" in new and isinstance(new["rows"], list):
            for v in new["rows"]:
                _merge(scan(str(v)))
            new["rows"] = scrub_value(new["rows"])
        if "sql" in new and isinstance(new["sql"], str):
            scrubbed, c = scrub_text(new["sql"])
            new["sql"] = scrubbed
            _merge(c)
        if "text" in new and isinstance(new["text"], str):
            scrubbed, c = scrub_text(new["text"])
            new["text"] = scrubbed
            _merge(c)
        if new.get("type") == "vega_lite" and isinstance(new.get("spec"), dict):
            spec = dict(new["spec"])
            data = dict(spec.get("data") or {})
            if isinstance(data.get("values"), list):
                for v in data["values"]:
                    _merge(scan(str(v)))
                data["values"] = scrub_value(data["values"])
                spec["data"] = data
                new["spec"] = spec
        if new.get("type") == "stats" and isinstance(new.get("columns"), dict):
            new_columns: dict[str, Any] = {}
            for col, col_stats in new["columns"].items():
                col_stats = dict(col_stats or {})
                top = col_stats.get("top")
                if isinstance(top, list):
                    new_top = []
                    for entry in top:
                        entry = dict(entry)
                        if isinstance(entry.get("value"), str):
                            scrubbed, c = scrub_text(entry["value"])
                            entry["value"] = scrubbed
                            _merge(c)
                        new_top.append(entry)
                    col_stats["top"] = new_top
                new_columns[col] = col_stats
            new["columns"] = new_columns
        scrubbed_artifacts.append(new)
    return {
        "mode": mode,
        "final_answer": scrubbed_answer,
        "artifacts": scrubbed_artifacts,
        "pii_counts": counts_total,
        "blocked": False,
    }


def scrub_for_storage(value: Any) -> Any:
    """Aplica scrub de PII para persistência (ex.: audit log), respeitando o
    modo configurado — só deixa passar sem mudanças quando o guard está
    desligado (`off`). Diferente de `apply_guard`, não bloqueia nem devolve
    metadata: é para texto/estruturas que precisam ser gravadas de qualquer
    forma (auditoria), mas nunca com PII em claro quando o guard está ativo.
    """
    if _resolve_mode() == MODE_OFF:
        return value
    return scrub_value(value)


__all__ = [
    "MODE_MASK", "MODE_BLOCK", "MODE_OFF",
    "scan", "scrub_text", "scrub_value", "scrub_for_storage", "apply_guard",
]
