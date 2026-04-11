from __future__ import annotations

from functools import partial

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.agents.document_build.nodes import (
    fetch_dataplex_tags,
    fetch_dbt_manifest,
    fetch_real_schema,
    finalize_document_markdown,
    generate_document_structure,
    parse_document_request,
)
from src.agents.document_build.state import DocumentBuildState


def build_graph(llm: BaseChatModel):
    workflow = StateGraph(DocumentBuildState)

    workflow.add_node("parse_document_request", parse_document_request)
    workflow.add_node("fetch_real_schema", fetch_real_schema)
    workflow.add_node("fetch_dataplex_tags", fetch_dataplex_tags)
    workflow.add_node("fetch_dbt_manifest", fetch_dbt_manifest)
    workflow.add_node(
        "generate_document_structure",
        partial(generate_document_structure, llm=llm),
    )
    workflow.add_node("finalize_document_markdown", finalize_document_markdown)

    workflow.add_edge(START, "parse_document_request")
    workflow.add_edge("parse_document_request", "fetch_real_schema")
    workflow.add_edge("fetch_real_schema", "fetch_dataplex_tags")
    workflow.add_edge("fetch_dataplex_tags", "fetch_dbt_manifest")
    workflow.add_edge("fetch_dbt_manifest", "generate_document_structure")
    workflow.add_edge("generate_document_structure", "finalize_document_markdown")
    workflow.add_edge("finalize_document_markdown", END)

    return workflow.compile()
