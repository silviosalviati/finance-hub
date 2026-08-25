from __future__ import annotations

from functools import partial
from typing import Any, Literal

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.agents.query_transformer.nodes import (
    analyze_input,
    architecture_gate,
    await_quality_approval,
    check_access,
    dry_run_generated,
    dry_run_original,
    evaluate_cost_efficiency,
    generate_sqlx,
    node_guardrails_in,
    node_guardrails_out,
    record_audit,
    score_quality,
    validate_sqlx_static,
    validate_equivalence,
)
from src.agents.query_transformer.state import QueryTransformerState


def _guard(state: QueryTransformerState) -> Literal["continue", "record_audit"]:
    """Desvia para o fan-in de auditoria se um nó anterior registrou erro —
    `record_audit` precisa rodar sempre, sucesso ou erro.
    """
    return "record_audit" if state.error else "continue"


def _guard_repairable(state: QueryTransformerState) -> Literal["continue", "repair", "record_audit"]:
    """Mesmo papel do `_guard`, mas dá uma segunda chance: erros marcados
    como `repairable_error` voltam pro `generate_sqlx` com o erro como
    contexto — uma única vez.
    """
    if not state.error:
        return "continue"
    if state.repairable_error and state.repair_attempts < 1:
        return "repair"
    return "record_audit"


def _route_after_quality(state: QueryTransformerState) -> Literal["guardrails_out", "generate_sqlx"]:
    """`await_quality_approval` já decide internamente se respeita o limite
    de 2 ciclos — aqui só roteia conforme a decisão (`human_decision`)."""
    return "generate_sqlx" if state.human_decision == "melhorar" else "guardrails_out"


def build_graph(llm: BaseChatModel, checkpointer: Any = None):
    workflow = StateGraph(QueryTransformerState)

    workflow.add_node("check_access", check_access)
    workflow.add_node("analyze_input", analyze_input)
    workflow.add_node("architecture_gate", architecture_gate)
    workflow.add_node("guardrails_in", node_guardrails_in)
    workflow.add_node("dry_run_original", dry_run_original)
    workflow.add_node("generate_sqlx", partial(generate_sqlx, llm=llm))
    workflow.add_node("validate_sqlx_static", validate_sqlx_static)
    workflow.add_node("dry_run_generated", dry_run_generated)
    workflow.add_node("validate_equivalence", validate_equivalence)
    workflow.add_node("evaluate_cost_efficiency", evaluate_cost_efficiency)
    workflow.add_node("score_quality", partial(score_quality, llm=llm))
    workflow.add_node("await_quality_approval", await_quality_approval)
    workflow.add_node("guardrails_out", node_guardrails_out)
    workflow.add_node("record_audit", record_audit)

    workflow.add_edge(START, "analyze_input")

    workflow.add_conditional_edges(
        "analyze_input",
        _guard,
        {"continue": "check_access", "record_audit": "record_audit"},
    )

    workflow.add_conditional_edges(
        "check_access",
        _guard,
        {"continue": "guardrails_in", "record_audit": "record_audit"},
    )
    workflow.add_conditional_edges(
        "guardrails_in",
        _guard,
        {"continue": "architecture_gate", "record_audit": "record_audit"},
    )
    workflow.add_conditional_edges(
        "architecture_gate",
        _guard,
        {"continue": "dry_run_original", "record_audit": "record_audit"},
    )
    workflow.add_conditional_edges(
        "dry_run_original",
        _guard,
        {"continue": "generate_sqlx", "record_audit": "record_audit"},
    )
    workflow.add_conditional_edges(
        "generate_sqlx",
        _guard,
        {"continue": "validate_sqlx_static", "record_audit": "record_audit"},
    )
    workflow.add_conditional_edges(
        "validate_sqlx_static",
        _guard_repairable,
        {
            "continue": "dry_run_generated",
            "repair": "generate_sqlx",
            "record_audit": "record_audit",
        },
    )

    # dry_run_generated: falha técnica do BigQuery ganha 1 retentativa (volta
    # pro generate_sqlx com o erro como contexto).
    workflow.add_conditional_edges(
        "dry_run_generated",
        _guard_repairable,
        {
            "continue": "validate_equivalence",
            "repair": "generate_sqlx",
            "record_audit": "record_audit",
        },
    )

    workflow.add_edge("validate_equivalence", "evaluate_cost_efficiency")
    workflow.add_edge("evaluate_cost_efficiency", "score_quality")
    workflow.add_edge("score_quality", "await_quality_approval")

    # score >= mínimo E equivalência ok (ou 2 ciclos já esgotados): segue.
    # "melhorar" com ciclo disponível: volta pro generate_sqlx com
    # score/issues/diff de equivalência como contexto.
    workflow.add_conditional_edges(
        "await_quality_approval",
        _route_after_quality,
        {"guardrails_out": "guardrails_out", "generate_sqlx": "generate_sqlx"},
    )

    workflow.add_edge("guardrails_out", "record_audit")
    workflow.add_edge("record_audit", END)

    return workflow.compile(checkpointer=checkpointer)
