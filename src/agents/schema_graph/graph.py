"""Grafo LangGraph do agente SchemaGraphExplorer.

Pipeline linear com guards de erro:

    START → discover_datasets → [guard] → discover_tables → [guard]
          → infer_relationships → enrich_with_llm → build_graph_payload → END
"""

from __future__ import annotations

from functools import partial
from typing import Literal

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


def _guard(state: SchemaGraphState) -> Literal["continue", "__end__"]:
    """Encerra o pipeline se um nó anterior registrou erro crítico."""
    return END if state.get("error") else "continue"


def build_graph(llm: BaseChatModel) -> StateGraph:
    """Constrói e compila o grafo SchemaGraphExplorer com o LLM fornecido."""
    workflow = StateGraph(SchemaGraphState)

    workflow.add_node("discover_datasets", discover_datasets)
    workflow.add_node("discover_tables", discover_tables)
    workflow.add_node("infer_relationships", infer_relationships)
    workflow.add_node("enrich_with_llm", partial(enrich_with_llm, llm=llm))
    workflow.add_node("build_graph_payload", build_graph_payload)

    workflow.add_edge(START, "discover_datasets")

    # Encerra se não for possível listar datasets do projeto
    workflow.add_conditional_edges(
        "discover_datasets",
        _guard,
        {"continue": "discover_tables", END: END},
    )

    # Encerra se nenhuma tabela for encontrada nos datasets
    workflow.add_conditional_edges(
        "discover_tables",
        _guard,
        {"continue": "infer_relationships", END: END},
    )

    workflow.add_edge("infer_relationships", "enrich_with_llm")
    workflow.add_edge("enrich_with_llm", "build_graph_payload")
    workflow.add_edge("build_graph_payload", END)

    return workflow.compile()
