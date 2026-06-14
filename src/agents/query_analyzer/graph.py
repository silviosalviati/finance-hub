from __future__ import annotations

from functools import partial
from typing import Any, Literal

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.agents.query_analyzer.nodes import (
    await_human_approval,
    detect_antipatterns,
    dry_run_baseline,
    enrich_with_intelligence,
    fetch_dataset_catalog,
    fetch_query_schema,
    optimize_query,
    parse_query,
    score_and_report,
    validate_data_existence,
    validate_optimized,
)
from src.agents.query_analyzer.state import AgentState


def _route_after_human(state: AgentState) -> Literal["optimize_query", "validate_data_existence"]:
    if state.human_decision == "skip":
        return "validate_data_existence"
    return "optimize_query" if state.needs_optimization else "validate_data_existence"


def _should_optimize(state: AgentState) -> Literal["optimize_query", "validate_data_existence"]:
    if state.needs_optimization and state.iteration < state.max_iterations:
        return "optimize_query"
    return "validate_data_existence"


def build_graph(
    llm: BaseChatModel,
    checkpointer: Any = None,
    llm_creative: BaseChatModel | None = None,
):
    """Constrói o grafo QueryAnalyzer.

    Topologia:
        parse_query
          → dry_run_baseline       (custo baseline)
          → fetch_query_schema     (schema das tabelas na query)
          → fetch_dataset_catalog  (catálogo completo do dataset + memória cross-sessão)
          → enrich_with_intelligence (LLM structured output: schema + catálogo + custo)
          → detect_antipatterns    (híbrido: regras + LLM structured output)
          → await_human_approval   (HITL)
          → optimize_query → validate_optimized ──(loop max Nx)──┐
          → validate_data_existence ←─────────────────────────────┘
          → score_and_report
    """
    _llm_report = llm_creative or llm
    workflow = StateGraph(AgentState)

    workflow.add_node("parse_query", parse_query)
    workflow.add_node("dry_run_baseline", dry_run_baseline)
    workflow.add_node("fetch_query_schema", fetch_query_schema)
    workflow.add_node("fetch_dataset_catalog", fetch_dataset_catalog)
    workflow.add_node("enrich_with_intelligence", partial(enrich_with_intelligence, llm=llm))
    workflow.add_node("detect_antipatterns", partial(detect_antipatterns, llm=llm))
    workflow.add_node("await_human_approval", await_human_approval)
    workflow.add_node("optimize_query", partial(optimize_query, llm=llm))
    workflow.add_node("validate_optimized", validate_optimized)
    workflow.add_node("validate_data_existence", validate_data_existence)
    workflow.add_node("score_and_report", partial(score_and_report, llm=_llm_report))

    workflow.add_edge(START, "parse_query")
    workflow.add_edge("parse_query", "dry_run_baseline")
    workflow.add_edge("dry_run_baseline", "fetch_query_schema")
    workflow.add_edge("fetch_query_schema", "fetch_dataset_catalog")
    workflow.add_edge("fetch_dataset_catalog", "enrich_with_intelligence")
    workflow.add_edge("enrich_with_intelligence", "detect_antipatterns")
    workflow.add_edge("detect_antipatterns", "await_human_approval")

    workflow.add_conditional_edges(
        "await_human_approval",
        _route_after_human,
        {
            "optimize_query": "optimize_query",
            "validate_data_existence": "validate_data_existence",
        },
    )

    workflow.add_edge("optimize_query", "validate_optimized")

    workflow.add_conditional_edges(
        "validate_optimized",
        _should_optimize,
        {
            "optimize_query": "optimize_query",
            "validate_data_existence": "validate_data_existence",
        },
    )

    workflow.add_edge("validate_data_existence", "score_and_report")
    workflow.add_edge("score_and_report", END)

    return workflow.compile(checkpointer=checkpointer)
