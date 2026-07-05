from __future__ import annotations

from functools import partial
from typing import Any, Literal

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.agents.query_build.nodes import (
    await_quality_approval,
    check_access,
    node_guardrails_in,
    node_guardrails_out,
    dry_run_generated_sql,
    fetch_generated_sample,
    generate_sql_from_request,
    record_audit,
    review_and_optimize_sql,
    score_query,
    validate_generated_sql_consistency,
)
from src.agents.query_build.state import QueryBuildState


def _guard(state: QueryBuildState) -> Literal["continue", "record_audit"]:
    """Desvia para o fan-in de auditoria se um nó anterior registrou erro —
    `record_audit` precisa rodar sempre, sucesso ou erro, então nenhum nó
    termina em END diretamente.
    """
    return "record_audit" if state.error else "continue"


def _guard_repairable(state: QueryBuildState) -> Literal["continue", "repair", "record_audit"]:
    """Mesmo papel do `_guard`, mas dá uma segunda chance: erros marcados
    como `repairable_error` (schema/sintaxe, não RBAC/SQL insegura) voltam
    pro `generate_sql` com o erro como contexto — uma única vez.
    """
    if not state.error:
        return "continue"
    if state.repairable_error and state.repair_attempts < 1:
        return "repair"
    return "record_audit"


def _route_after_quality(state: QueryBuildState) -> Literal["sample_generated", "generate_sql"]:
    """`await_quality_approval` já decide internamente se respeita o limite
    de 2 ciclos — aqui só roteia conforme a decisão (`human_decision`)."""
    return "generate_sql" if state.human_decision == "melhorar" else "sample_generated"


def build_graph(llm: BaseChatModel, checkpointer: Any = None):
    workflow = StateGraph(QueryBuildState)

    workflow.add_node("check_access", check_access)
    workflow.add_node("guardrails_in", node_guardrails_in)
    workflow.add_node("generate_sql", partial(generate_sql_from_request, llm=llm))
    workflow.add_node("review_sql", partial(review_and_optimize_sql, llm=llm))
    workflow.add_node("validate_sql", validate_generated_sql_consistency)
    workflow.add_node("dry_run_generated", dry_run_generated_sql)
    workflow.add_node("score_query", partial(score_query, llm=llm))
    workflow.add_node("await_quality_approval", await_quality_approval)
    workflow.add_node("sample_generated", fetch_generated_sample)
    workflow.add_node("guardrails_out", node_guardrails_out)
    workflow.add_node("record_audit", record_audit)

    workflow.add_edge(START, "check_access")

    # Bloqueia antes de gastar qualquer chamada de LLM se o RBAC reprovar.
    workflow.add_conditional_edges(
        "check_access",
        _guard,
        {"continue": "guardrails_in", "record_audit": "record_audit"},
    )

    # Valida prompt injection antes de chamar LLM.
    workflow.add_conditional_edges(
        "guardrails_in",
        _guard,
        {"continue": "generate_sql", "record_audit": "record_audit"},
    )

    # Encerra se generate_sql falhou (tabela inválida, schema ausente, etc.)
    workflow.add_conditional_edges(
        "generate_sql",
        _guard,
        {"continue": "review_sql", "record_audit": "record_audit"},
    )

    workflow.add_edge("review_sql", "validate_sql")

    # validate_sql: erro recuperável (placeholder/coluna) volta 1x pro generate_sql;
    # SQL insegura ou erro já repetido vai direto pra auditoria.
    workflow.add_conditional_edges(
        "validate_sql",
        _guard_repairable,
        {
            "continue": "dry_run_generated",
            "repair": "generate_sql",
            "record_audit": "record_audit",
        },
    )

    # dry_run_generated: falha técnica do BigQuery também ganha 1 retentativa;
    # orçamento excedido vai direto pra auditoria (regenerar SQL não reduz o escopo pedido).
    workflow.add_conditional_edges(
        "dry_run_generated",
        _guard_repairable,
        {
            "continue": "score_query",
            "repair": "generate_sql",
            "record_audit": "record_audit",
        },
    )

    workflow.add_edge("score_query", "await_quality_approval")

    # score >= mínimo (ou 2 ciclos de "melhorar" já esgotados): segue pra amostra.
    # "melhorar" com ciclo disponível: volta pro generate_sql com score/issues como contexto.
    workflow.add_conditional_edges(
        "await_quality_approval",
        _route_after_quality,
        {"sample_generated": "sample_generated", "generate_sql": "generate_sql"},
    )

    workflow.add_edge("sample_generated", "guardrails_out")
    workflow.add_edge("guardrails_out", "record_audit")
    workflow.add_edge("record_audit", END)

    return workflow.compile(checkpointer=checkpointer)
