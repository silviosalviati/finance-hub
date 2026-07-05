"""Prompt injection guard — detecta padrões comuns de jailbreak/prompt-override."""

from __future__ import annotations

_INJECTION_MARKERS = (
    "ignore previous",
    "ignore as instru",
    "desconsidere as instru",
    "system prompt",
    "</system>",
)


def check_injection(text: str, markers: tuple[str, ...] | None = None) -> tuple[bool, str | None]:
    """Valida text contra marcadores de prompt injection.

    Args:
        text: texto da requisição do usuário
        markers: tuple de padrões a verificar (default: _INJECTION_MARKERS)

    Returns:
        (safe: bool, reason: str | None)
            - (True, None): texto limpo, sem markers detectados
            - (False, f"Padrão detectado: {marker}"): injection detectada
    """
    if not text:
        return True, None

    markers = markers or _INJECTION_MARKERS
    text_lower = text.lower()

    for marker in markers:
        if marker in text_lower:
            return False, f"Padrão detectado: {marker}"

    return True, None
