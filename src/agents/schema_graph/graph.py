"""Grafo LangGraph do agente SchemaGraphExplorer.

Pipeline linear:

    START → discover_datasets → discover_tables → infer_relationships
          → enrich_with_llm → build_graph_payload → END
"""

from __future__ import annotations

from functools import partial

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.agents.schema_graph.nodes import (
    build_graph_payload,
    discover_datasets,
    discover_tables,
    enrich_with_llm,
    infer_relationships,
)
from src.agents.schema_graph.state import SchemaGraphState


def build_graph(llm: BaseChatModel) -> StateGraph:
    """Constrói e compila o grafo SchemaGraphExplorer com o LLM fornecido."""
    workflow = StateGraph(SchemaGraphState)

    workflow.add_node("discover_datasets", discover_datasets)
    workflow.add_node("discover_tables", discover_tables)
    workflow.add_node("infer_relationships", infer_relationships)
    workflow.add_node("enrich_with_llm", partial(enrich_with_llm, llm=llm))
    workflow.add_node("build_graph_payload", build_graph_payload)

    workflow.add_edge(START, "discover_datasets")
    workflow.add_edge("discover_datasets", "discover_tables")
    workflow.add_edge("discover_tables", "infer_relationships")
    workflow.add_edge("infer_relationships", "enrich_with_llm")
    workflow.add_edge("enrich_with_llm", "build_graph_payload")
    workflow.add_edge("build_graph_payload", END)

    return workflow.compile()
