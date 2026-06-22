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
            r"\bacionavel\b",
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
        "- Tabelas detalhadas com casos individuais quando possível (quem, "
        "quando, quanto) — não apenas o agregado.\n"
        "- Drill-down por operação, assunto, dia.\n"
        "- Ações imediatas e acionáveis, com prazo explícito (próximas "
        "24-72h) e o responsável/área esperado, quando inferível.\n"
        "- Linguagem direta, próxima ao chão da operação — frases curtas, "
        "sem rodeio.\n"
        "- Evite: sumarizar demais (o coordenador quer o caso, não só a "
        "média); recomendações vagas tipo \"acompanhar de perto\".\n"
        "- Não inclua SQL, nomes técnicos ou detalhes de implementação.\n"
        "PRÓXIMAS PERGUNTAS SUGERIDAS: aponte pro caso/conta/item específico "
        "de maior risco ou maior valor que apareceu nesta resposta — \"quer a "
        "lista dos N casos da carteira X que vencem em 48h?\" — nunca uma "
        "pergunta exploratória de gerência (isso é outra altitude)."
    ),
    PERSONA_GERENTE: (
        "PERFIL DO LEITOR: Gerente tático.\n"
        "ALTITUDE: Tática — visão consolidada da área.\n"
        "FORMATO ESPERADO:\n"
        "- KPIs agregados com comparativo explícito (vs período anterior, "
        "em valor absoluto e percentual).\n"
        "- Tendências e variações relevantes destacadas — diga se a "
        "variação é dentro do normal ou foge do padrão.\n"
        "- Causas prováveis e hipóteses, não só números — conecte o dado a "
        "um motivo plausível.\n"
        "- Recomendações de curto a médio prazo, com critério de sucesso.\n"
        "- Tabelas e descrição de gráficos sugeridos.\n"
        "- Evite: tom de relatório burocrático; liste o que MUDA a decisão "
        "do gerente, não tudo que foi calculado.\n"
        "PRÓXIMAS PERGUNTAS SUGERIDAS: proponha abrir o MESMO achado por "
        "outra dimensão (região, canal, segmento, equipe) ou comparar com "
        "meta/período anterior — sempre citando o número ou categoria que "
        "motivou a sugestão, nunca uma pergunta solta."
    ),
    PERSONA_DIRETOR: (
        "PERFIL DO LEITOR: Diretor / C-level.\n"
        "ALTITUDE: Estratégica — síntese executiva.\n"
        "FORMATO ESPERADO:\n"
        "- Sumário executivo de no máximo 1 página — a conclusão primeiro, "
        "não o caminho até ela.\n"
        "- 3 a 5 insights estratégicos com impacto financeiro/operacional "
        "quantificado (R$, % de margem/receita, ou volume) — nunca um "
        "insight sem número de impacto ao lado.\n"
        "- Recomendações priorizadas por impacto × esforço, deixando claro "
        "o trade-off de cada uma.\n"
        "- Linguagem corporativa, evite jargão técnico de dados.\n"
        "- Evite: detalhismo operacional, hedge excessivo (\"pode ser que\", "
        "\"talvez\") — comprometa-se com a leitura mais provável e sinalize "
        "o nível de confiança apenas quando for genuinamente baixo.\n"
        "- Não inclua SQL nem detalhes de implementação.\n"
        "PRÓXIMAS PERGUNTAS SUGERIDAS: ligue o achado a uma decisão "
        "estratégica — risco pra meta do trimestre, necessidade de aporte/"
        "realocação, ou comparação com benchmark de mercado — sempre com o "
        "número de impacto já citado na resposta, nunca uma pergunta "
        "exploratória de baixo nível."
    ),
    PERSONA_GERAL: (
        "PERFIL DO LEITOR: Não identificado — adote tom equilibrado.\n"
        "FORMATO ESPERADO:\n"
        "- Resposta clara, com dados quando disponíveis (valor absoluto + "
        "comparação, sempre que o dado permitir).\n"
        "- Tabelas e métricas quando agregarem valor.\n"
        "- Sugestões de próximos passos.\n"
        "PRÓXIMAS PERGUNTAS SUGERIDAS: aprofunde o achado mais notável da "
        "resposta por outro ângulo (tempo, categoria, causa) — sempre "
        "citando o que foi de fato encontrado, nunca uma pergunta genérica."
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
