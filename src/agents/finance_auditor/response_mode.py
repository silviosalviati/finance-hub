"""Response Mode Resolver — escolhe a estrutura da resposta (não a altitude).

Persona (`personas.py`) decide PARA QUEM a resposta é escrita (altitude:
coordenador/gerente/diretor). Response Mode decide EM QUE FORMATO ela deve
ser organizada, a partir da intenção da pergunta:

    padrao           → formato padrão do Composer (resumo/achados/tabela).
    analise_profunda → diagnóstico estruturado (fato → causa raiz → impacto
                        → solução → priorização), para quando o usuário
                        pede para entender o "porquê", não só o número.

São dois eixos independentes — um diretor também pode pedir uma análise
profunda — por isso vivem em nós separados do grafo.
"""

from __future__ import annotations

import re
import unicodedata

RESPONSE_MODE_PADRAO = "padrao"
RESPONSE_MODE_ANALISE_PROFUNDA = "analise_profunda"

VALID_RESPONSE_MODES = (RESPONSE_MODE_PADRAO, RESPONSE_MODE_ANALISE_PROFUNDA)

_DEEP_ANALYSIS_PATTERNS: tuple[str, ...] = (
    r"\banalis[ae]\s+profund",
    r"\banalise\s+completa\b",
    r"\bdiagn[oó]stico\b",
    r"\bcausa\s+raiz\b",
    r"\bra[ií]z\s+do\s+problema\b",
    r"\bo\s+que\s+aconteceu\b",
    r"\bpor\s*qu[eê]\s+(?:isso\s+|isto\s+)?aconteceu\b",
    r"\bentenda\s+o\s+motivo\b",
    r"\bentender\s+o\s+motivo\b",
    r"\binvestiga[cç][aã]o\s+detalhada\b",
    r"\binvestigar\s+a\s+fundo\b",
    r"\bplano\s+de\s+a[cç][aã]o\b",
    r"\bo\s+que\s+priorizar\b",
)


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text or "")
        if unicodedata.category(c) != "Mn"
    )


def _normalize(text: str) -> str:
    return _strip_accents((text or "").lower())


def detect_response_mode(query: str) -> str:
    """Detecta o modo de resposta a partir do texto da pergunta.

    Heurística textual — mesma filosofia de `personas.detect_persona`: sem
    custo de LLM, suficiente para decidir a estrutura da resposta.
    """
    content = _normalize(query)
    for pattern in _DEEP_ANALYSIS_PATTERNS:
        if re.search(pattern, content, flags=re.IGNORECASE):
            return RESPONSE_MODE_ANALISE_PROFUNDA
    return RESPONSE_MODE_PADRAO


RESPONSE_MODE_PROMPTS: dict[str, str] = {
    RESPONSE_MODE_ANALISE_PROFUNDA: (
        "MODO DE RESPOSTA: Análise profunda (diagnóstico estruturado).\n"
        "Ignore o formato padrão de 4 seções abaixo. Em vez disso, estruture "
        "a resposta OBRIGATORIAMENTE nestas seções, nesta ordem, com estes "
        "títulos exatos:\n"
        "## O que aconteceu?\n"
        "O fato observado nos dados — números concretos (absolutos e "
        "variação percentual), com o período analisado explícito.\n"
        "## Por que aconteceu?\n"
        "A causa raiz mais provável, cruzando os achados disponíveis. Não "
        "invente causa sem evidência nos dados — se a causa exata não puder "
        "ser comprovada com o que foi coletado, apresente as 2-3 hipóteses "
        "mais plausíveis em ordem de probabilidade e diga objetivamente o "
        "que precisaria ser investigado a mais para confirmar.\n"
        "## Qual o impacto?\n"
        "O efeito/consequência quantificado (financeiro, operacional ou de "
        "cliente) sempre que os dados permitirem — evite \"impacto "
        "relevante\" sem número ao lado.\n"
        "## O que fazer?\n"
        "A(s) solução(ões) mais diretas para a causa raiz identificada — "
        "não para o sintoma.\n"
        "## O que priorizar?\n"
        "Um plano de ação com 2 a 4 passos, ordenados por impacto × "
        "urgência/esforço.\n"
        "Não pule nenhuma das 5 seções, mesmo que a resposta de alguma seja "
        "curta. Ao final, mantenha também a seção "
        "`## Próximas perguntas sugeridas` com 3 sugestões, igual ao formato "
        "padrão."
    ),
    RESPONSE_MODE_PADRAO: "",
}


def get_response_mode_prompt(mode: str) -> str:
    """Retorna o bloco de prompt do Composer correspondente ao modo."""
    return RESPONSE_MODE_PROMPTS.get(mode, "")


__all__ = [
    "RESPONSE_MODE_PADRAO",
    "RESPONSE_MODE_ANALISE_PROFUNDA",
    "VALID_RESPONSE_MODES",
    "detect_response_mode",
    "get_response_mode_prompt",
    "RESPONSE_MODE_PROMPTS",
]
