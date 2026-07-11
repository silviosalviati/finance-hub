"""Agentic RAG: grading, query transformation e retry automático para
o catalog_search do Finance Voice IA.

Fecha o loop que o RAG passivo deixava aberto:
    retrieve → grade → (reescreve se ruim) → retry → passa ao gerador

Sem isso, o Planner consumia contexto ruim (tabelas erradas) em silêncio
quando o nome do dataset não tinha relação textual com seu conteúdo.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.finance_auditor.catalog_index import search_catalog
from src.shared.tools.llm import invoke_with_retry

_logger = logging.getLogger(__name__)

_GRADE_THRESHOLD_DEFAULT = 0.65  # abaixo disso → tenta reescrita + retry
_GRADE_FALLBACK = 0.50           # abaixo disso após retry → avança com log warning

_TRANSFORM_SYSTEM = """\
Você é um especialista em busca semântica sobre catálogos de dados BigQuery.
Dada uma pergunta de negócio, gere 2 a 3 variações de consulta de busca
focadas em diferentes aspectos técnicos e de negócio que ajudem a localizar
as tabelas e colunas certas no catálogo.

Responda SOMENTE em JSON válido:
{"queries": ["<variação 1>", "<variação 2>", "<variação 3>"]}

Sem texto adicional, sem markdown fence, sem comentários."""


def grade_catalog_results(results: list[dict[str, Any]]) -> float:
    """Score médio dos top-3 resultados retornados pelo search_catalog.

    Retorna 0.0 se a lista estiver vazia.
    """
    if not results:
        return 0.0
    top = results[:3]
    return sum(r.get("score", 0.0) for r in top) / len(top)


def transform_query(
    query: str,
    llm: BaseChatModel,
    usage_sink: list[dict[str, Any]] | None = None,
) -> list[str]:
    """Gera 2-3 variações de query para melhorar o recall semântico do catálogo.

    Exemplo:
        'contas a receber em atraso março 2025' →
        ['inadimplência aging receivables vencidos',
         'títulos vencidos data vencimento contas receber',
         'overdue accounts receivable payment due date']

    Retorna lista vazia em caso de falha (degradação graceful) — inclusive se
    o orçamento de tokens da requisição já tiver sido excedido
    (`TokenBudgetExceeded`), capturado pelo `except Exception` abaixo.
    """
    try:
        resp = invoke_with_retry(
            llm,
            [
                SystemMessage(content=_TRANSFORM_SYSTEM),
                HumanMessage(content=f"Pergunta original: {query}"),
            ],
            max_attempts=2,
            label="agentic_retrieval_transform_query",
            usage_sink=usage_sink,
        )
        raw = str(getattr(resp, "content", resp) or "").strip()
        # Tolerância a markdown fence ocasional do LLM
        if "```" in raw:
            raw = raw.split("```")[1].lstrip("json").strip()
        data = json.loads(raw)
        queries = [str(q).strip() for q in (data.get("queries") or []) if str(q).strip()]
        return queries[:3]
    except Exception:  # noqa: BLE001
        _logger.debug(
            "agentic_retrieval.transform_query falhou para '%s' — sem variações", query
        )
        return []


def adaptive_search_catalog(
    project_id: str,
    query: str,
    llm: BaseChatModel | None = None,
    top_k: int = 5,
    grade_threshold: float = _GRADE_THRESHOLD_DEFAULT,
    usage_sink: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Busca semântica com grading e retry automático.

    Fluxo:
        1. Busca com a query original.
        2. Avalia o score médio dos top-3 resultados.
        3. Se score >= grade_threshold ou sem LLM → retorna imediatamente.
        4. Se score < grade_threshold → reescreve a query via LLM (2-3 variações)
           e busca para cada uma; mescla e re-ordena todos os resultados.
        5. Se ainda abaixo de _GRADE_FALLBACK → log warning e retorna o melhor
           disponível (nunca falha, sempre devolve algo).

    ``llm=None`` desativa o grading e a reescrita — comportamento idêntico ao
    ``search_catalog`` anterior (compatibilidade retroativa).
    """
    results = search_catalog(project_id, query, top_k=top_k)
    grade = grade_catalog_results(results)

    _logger.debug(
        "adaptive_search_catalog '%s': grade=%.3f threshold=%.2f",
        query, grade, grade_threshold,
    )

    if grade >= grade_threshold or llm is None:
        return results

    # Grade insuficiente — tenta variações de query
    variations = transform_query(query, llm, usage_sink=usage_sink)
    if not variations:
        return results

    # Mescla todos os resultados, mantendo o maior score por tabela
    best: dict[str, dict[str, Any]] = {r["full_name"]: r for r in results}
    for variation in variations:
        for r in search_catalog(project_id, variation, top_k=top_k):
            key = r["full_name"]
            if key not in best or r["score"] > best[key]["score"]:
                best[key] = r

    merged = sorted(best.values(), key=lambda x: x.get("score", 0.0), reverse=True)
    merged_grade = grade_catalog_results(merged)

    if merged_grade < _GRADE_FALLBACK:
        _logger.warning(
            "adaptive_search_catalog: grade %.3f abaixo do fallback %.2f "
            "para query '%s' — retornando melhores disponíveis",
            merged_grade, _GRADE_FALLBACK, query,
        )

    return merged[:top_k]


__all__ = ["grade_catalog_results", "transform_query", "adaptive_search_catalog"]
