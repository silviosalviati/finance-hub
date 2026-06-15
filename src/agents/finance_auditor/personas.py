"""Persona Resolver — adapta altitude e formato da resposta ao consumidor.

Detecta a persona do usuário (Coordenador / Gerente / Diretor / Geral) a partir
do texto da pergunta e do perfil persistido na sessão, e devolve o prompt
adequado para o Composer.

Heurística simples baseada em padrões textuais — fase 1. Em fases seguintes
pode ser substituída por classificador LLM ou por role vindo do RBAC.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

PERSONA_COORDENADOR = "coordenador"
PERSONA_GERENTE = "gerente"
PERSONA_DIRETOR = "diretor"
PERSONA_GERAL = "geral"

VALID_PERSONAS = (
    PERSONA_COORDENADOR,
    PERSONA_GERENTE,
    PERSONA_DIRETOR,
    PERSONA_GERAL,
)

_PERSONA_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        PERSONA_DIRETOR,
        (
            r"\bdiretor(?:ia|es)?\b",
            r"\bvis[aã]o\s+executiv",
            r"\bestrat[eé]gic",
            r"\bc[- ]?level\b",
            r"\bboard\b",
            r"\broi\b",
            r"\bimpacto\s+financeiro\b",
            r"\bsumario\s+executivo\b",
            r"\bsum[aá]rio\s+executivo\b",
        ),
    ),
    (
        PERSONA_GERENTE,
        (
            r"\bgerent(?:e|es|ial)\b",
            r"\bt[aá]tic",
            r"\bcompar(?:ar|ativo|a[cç][aã]o)",
            r"\btend[eê]ncia",
            r"\bkpi",
            r"\bdashboard",
            r"\bm[eê]s\s+anterior\b",
            r"\bmom\b",
            r"\byoy\b",
        ),
    ),
    (
        PERSONA_COORDENADOR,
        (
            r"\bcoordenador",
            r"\boperacional\b",
            r"\bdrill[- ]?down\b",
            r"\bcaso\s+a\s+caso\b",
            r"\bdetalhe\s+por\b",
            r"\blista\s+(?:os|as)\b",
            r"\btop\s+\d+\b",
            r"\bacionable\b",
            r"\bsupervis(?:or|ao)\b",
        ),
    ),
)


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def _normalize(text: str) -> str:
    return _strip_accents((text or "").lower())


def detect_persona(query: str, profile: dict[str, Any] | None = None) -> str:
    """Detecta a persona a partir da pergunta e do perfil da sessão.

    Ordem de precedência:
    1. `profile.persona` se já definido e válido (sticky por sessão).
    2. Padrões textuais explícitos na pergunta atual.
    3. Fallback `geral`.
    """
    if profile:
        sticky = str(profile.get("persona") or "").strip().lower()
        if sticky in VALID_PERSONAS:
            return sticky

    content = _normalize(query)
    for persona, patterns in _PERSONA_PATTERNS:
        for pattern in patterns:
            if re.search(pattern, content, flags=re.IGNORECASE):
                return persona

    return PERSONA_GERAL


PERSONA_PROMPTS: dict[str, str] = {
    PERSONA_COORDENADOR: (
        "PERFIL DO LEITOR: Coordenador operacional.\n"
        "ALTITUDE: Operacional — foco no dia a dia da operação.\n"
        "FORMATO ESPERADO:\n"
        "- Tabelas detalhadas com casos individuais quando possível.\n"
        "- Drill-down por operação, assunto, dia.\n"
        "- Ações imediatas e acionáveis (próximas 24-72h).\n"
        "- Linguagem direta, próxima ao chão da operação.\n"
        "- Inclua SQL gerado quando relevante para reproduzir."
    ),
    PERSONA_GERENTE: (
        "PERFIL DO LEITOR: Gerente tático.\n"
        "ALTITUDE: Tática — visão consolidada da área.\n"
        "FORMATO ESPERADO:\n"
        "- KPIs agregados com comparativos (vs período anterior).\n"
        "- Tendências e variações relevantes destacadas.\n"
        "- Causas prováveis e hipóteses, não só números.\n"
        "- Recomendações de curto a médio prazo.\n"
        "- Tabelas e descrição de gráficos sugeridos."
    ),
    PERSONA_DIRETOR: (
        "PERFIL DO LEITOR: Diretor / C-level.\n"
        "ALTITUDE: Estratégica — síntese executiva.\n"
        "FORMATO ESPERADO:\n"
        "- Sumário executivo de no máximo 1 página.\n"
        "- 3 a 5 insights estratégicos com impacto financeiro/operacional.\n"
        "- Recomendações priorizadas por impacto × esforço.\n"
        "- Linguagem corporativa, evite jargão técnico de dados.\n"
        "- Não inclua SQL nem detalhes de implementação."
    ),
    PERSONA_GERAL: (
        "PERFIL DO LEITOR: Não identificado — adote tom equilibrado.\n"
        "FORMATO ESPERADO:\n"
        "- Resposta clara, com dados quando disponíveis.\n"
        "- Tabelas e métricas quando agregarem valor.\n"
        "- Sugestões de próximos passos."
    ),
}


def get_persona_prompt(persona: str) -> str:
    """Retorna o bloco de prompt do Composer correspondente à persona."""
    return PERSONA_PROMPTS.get(persona, PERSONA_PROMPTS[PERSONA_GERAL])


__all__ = [
    "PERSONA_COORDENADOR",
    "PERSONA_GERENTE",
    "PERSONA_DIRETOR",
    "PERSONA_GERAL",
    "VALID_PERSONAS",
    "detect_persona",
    "get_persona_prompt",
    "PERSONA_PROMPTS",
]
