"""Prompt injection guard — detecta padrões comuns de jailbreak/prompt-override.

Normaliza acentos e usa regex com `\\b`/espaçamento flexível em vez de
substring puro — resiste a pequenas variações de escrita ("instrucoes" vs
"instruções", pontuação/espaço extra) que um `in text.lower()` simples deixa
passar. Continua sendo heurística (não é classificação por LLM) — reduz o
espaço de bypass óbvio, não elimina prompt injection.
"""

from __future__ import annotations

import re
import unicodedata


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFD", text or "")
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text.lower()


_INJECTION_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p)
    for p in (
        r"ignor[ae]\s+(as\s+|all\s+)?instru",
        r"ignore\s+(all\s+)?previous",
        r"desconsider[ae]\s+(as\s+)?instru",
        r"esqueca\s+(suas\s+|as\s+)?instru",
        r"disregard\s+(the\s+)?(above|previous)",
        r"forget\s+(your\s+|all\s+)?(previous\s+)?instru",
        r"system\s*prompt",
        r"</?\s*system\s*>",
        r"you\s+are\s+now\s+(a|an)\b",
        r"aja\s+como\s+(se\s+)?(um|uma)?\s*(assistente\s+sem|sem\s+restri)",
        r"a\s+partir\s+de\s+agora\s+voce\s+e\b",
        r"reveal\s+(your\s+)?(system\s+)?prompt",
        r"revele\s+(seu|o)\s+prompt",
        r"print\s+your\s+(system\s+)?instructions",
        r"modo\s+desenvolvedor",
        r"developer\s+mode",
        r"jailbreak",
        r"\bdan\s+mode\b",
        r"override\s+(your\s+|the\s+)?(system\s+)?instru",
        r"sem\s+restricoes\s+(agora|a\s+partir)",
    )
)


def check_injection(
    text: str, patterns: tuple[re.Pattern[str], ...] | None = None
) -> tuple[bool, str | None]:
    """Valida text contra padrões de prompt injection.

    Args:
        text: texto da requisição do usuário
        patterns: tuple de regex compiladas a verificar (default: _INJECTION_PATTERNS)

    Returns:
        (safe: bool, reason: str | None)
            - (True, None): texto limpo, sem padrão detectado
            - (False, f"Padrão detectado: {pattern}"): injection detectada
    """
    if not text:
        return True, None

    patterns = patterns or _INJECTION_PATTERNS
    normalized = _normalize(text)

    for pat in patterns:
        if pat.search(normalized):
            return False, f"Padrão detectado: {pat.pattern}"

    return True, None
