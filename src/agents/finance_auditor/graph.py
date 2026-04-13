"""Grafo LangGraph do agente FinanceAuditor.

Topologia (fan-out paralelo + fan-in):

    START → fetch_data → node_sentiment ─┐
                       → node_friction  ─┼→ consolidate_metrics → report_generator → END
                       → node_themes    ─┘
"""

from __future__ import annotations

from functools import partial

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.agents.finance_auditor.nodes import (
    consolidate_metrics,
    fetch_data,
    node_friction,
    node_sentiment,
    node_themes,
    report_generator,
)
from src.agents.finance_auditor.state import FinanceAuditorState


def build_graph(llm: BaseChatModel) -> StateGraph:
    """Constrói e compila o grafo FinanceAuditor com o LLM fornecido."""
    workflow = StateGraph(FinanceAuditorState)

    # Registra nós
    workflow.add_node("fetch_data", partial(fetch_data, llm=llm))
    workflow.add_node("node_sentiment", node_sentiment)
    workflow.add_node("node_friction", node_friction)
    workflow.add_node("node_themes", partial(node_themes, llm=llm))
    workflow.add_node("consolidate_metrics", consolidate_metrics)
    workflow.add_node("report_generator", partial(report_generator, llm=llm))

    # Arestas de entrada e fan-out
    workflow.add_edge(START, "fetch_data")
    workflow.add_edge("fetch_data", "node_sentiment")
    workflow.add_edge("fetch_data", "node_friction")
    workflow.add_edge("fetch_data", "node_themes")

    # Fan-in → consolidação → relatório → fim
    workflow.add_edge("node_sentiment", "consolidate_metrics")
    workflow.add_edge("node_friction", "consolidate_metrics")
    workflow.add_edge("node_themes", "consolidate_metrics")
    workflow.add_edge("consolidate_metrics", "report_generator")
    workflow.add_edge("report_generator", END)

    return workflow.compile()
