from __future__ import annotations

from functools import partial

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.agents.query_build.nodes import (
    dry_run_generated_sql,
    fetch_generated_sample,
    generate_sql_from_request,
)
from src.agents.query_build.state import QueryBuildState


def build_graph(llm: BaseChatModel):
    workflow = StateGraph(QueryBuildState)

    workflow.add_node("generate_sql", partial(generate_sql_from_request, llm=llm))
    workflow.add_node("dry_run_generated", dry_run_generated_sql)
    workflow.add_node("sample_generated", fetch_generated_sample)

    workflow.add_edge(START, "generate_sql")
    workflow.add_edge("generate_sql", "dry_run_generated")
    workflow.add_edge("dry_run_generated", "sample_generated")
    workflow.add_edge("sample_generated", END)

    return workflow.compile()
