from __future__ import annotations

from functools import partial
from typing import Literal

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.agents.query_analyzer.nodes import (
    analyze_patterns,
    dry_run_estimate,
    generate_report,
    optimize_query,
    parse_query,
    validate_optimized,
)
from src.agents.query_analyzer.state import AgentState


def should_optimize(state: AgentState) -> Literal["optimize_query", "generate_report"]:
    needs_opt = getattr(state, "needs_optimization", False)
    iter_count = getattr(state, "iteration", 0)
    max_iters = getattr(state, "max_iterations", 2)

    if needs_opt and iter_count < max_iters:
        return "optimize_query"

    return "generate_report"


def should_reoptimize(state: AgentState) -> Literal["optimize_query", "generate_report"]:
    needs_opt = getattr(state, "needs_optimization", False)
    iter_count = getattr(state, "iteration", 0)
    max_iters = getattr(state, "max_iterations", 3)

    if needs_opt and iter_count < max_iters:
        return "optimize_query"

    return "generate_report"


def build_graph(llm: BaseChatModel):
    workflow = StateGraph(AgentState)

    workflow.add_node("parse_query", parse_query)
    workflow.add_node("dry_run_estimate", dry_run_estimate)
    workflow.add_node("analyze_patterns", partial(analyze_patterns, llm=llm))
    workflow.add_node("optimize_query", partial(optimize_query, llm=llm))
    workflow.add_node("validate_optimized", validate_optimized)
    workflow.add_node("generate_report", partial(generate_report, llm=llm))

    workflow.add_edge(START, "parse_query")
    workflow.add_edge("parse_query", "dry_run_estimate")
    workflow.add_edge("dry_run_estimate", "analyze_patterns")

    workflow.add_conditional_edges(
        "analyze_patterns",
        should_optimize,
        {
            "optimize_query": "optimize_query",
            "generate_report": "generate_report",
        },
    )

    workflow.add_edge("optimize_query", "validate_optimized")
    workflow.add_conditional_edges(
        "validate_optimized",
        should_reoptimize,
        {
            "optimize_query": "optimize_query",
            "generate_report": "generate_report",
        },
    )
    workflow.add_edge("generate_report", END)

    return workflow.compile()
