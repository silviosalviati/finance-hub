from __future__ import annotations

from functools import partial
from typing import Any, Literal

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.agents.query_analyzer.nodes import (
    analyze_patterns,
    await_human_approval,
    dry_run_estimate,
    generate_report,
    optimize_query,
    parse_query,
    validate_optimized,
)
from src.agents.query_analyzer.state import AgentState


def _route_after_human(state: AgentState) -> Literal["optimize_query", "generate_report"]:
    """Roteia com base na decisão humana.

    'skip' (ou ausência de otimização necessária) → relatório direto.
    Qualquer outra decisão ('approve', texto de feedback, etc.) → otimização.
    """
    if state.human_decision == "skip":
        return "generate_report"
    return "optimize_query" if state.needs_optimization else "generate_report"


def _should_optimize(state: AgentState) -> Literal["optimize_query", "generate_report"]:
    """Controla o loop de re-otimização após validate_optimized."""
    if state.needs_optimization and state.iteration < state.max_iterations:
        return "optimize_query"
    return "generate_report"


def build_graph(
    llm: BaseChatModel,
    checkpointer: Any = None,
    llm_creative: BaseChatModel | None = None,
):
    """Constrói o grafo QueryAnalyzer.

    Args:
        llm: LLM analítico (baixa temperatura) — análise e otimização.
        checkpointer: Checkpointer para persistência HITL.
        llm_creative: LLM criativo (temperatura maior) — geração do relatório.
                      Cai para `llm` quando não informado.
    """
    _llm_report = llm_creative or llm
    workflow = StateGraph(AgentState)

    workflow.add_node("parse_query", parse_query)
    workflow.add_node("dry_run_estimate", dry_run_estimate)
    workflow.add_node("analyze_patterns", partial(analyze_patterns, llm=llm))
    workflow.add_node("await_human_approval", await_human_approval)
    workflow.add_node("optimize_query", partial(optimize_query, llm=llm))
    workflow.add_node("validate_optimized", validate_optimized)
    workflow.add_node("generate_report", partial(generate_report, llm=_llm_report))

    workflow.add_edge(START, "parse_query")
    workflow.add_edge("parse_query", "dry_run_estimate")
    workflow.add_edge("dry_run_estimate", "analyze_patterns")
    workflow.add_edge("analyze_patterns", "await_human_approval")

    # Após aprovação humana: otimizar ou ir direto ao relatório
    workflow.add_conditional_edges(
        "await_human_approval",
        _route_after_human,
        {
            "optimize_query": "optimize_query",
            "generate_report": "generate_report",
        },
    )

    workflow.add_edge("optimize_query", "validate_optimized")

    # Loop de re-otimização: repete até atingir qualidade ou max_iterations
    workflow.add_conditional_edges(
        "validate_optimized",
        _should_optimize,
        {
            "optimize_query": "optimize_query",
            "generate_report": "generate_report",
        },
    )

    workflow.add_edge("generate_report", END)

    return workflow.compile(checkpointer=checkpointer)
