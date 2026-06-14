"""Estado do agente SchemaGraphExplorer."""

from __future__ import annotations

import operator
from typing import Annotated, Any, TypedDict


class SchemaGraphState(TypedDict, total=False):
    """Estado compartilhado do grafo SchemaGraphExplorer.

    Campos opcionais (total=False) para que os nós possam atualizar
    chaves distintas sem conflito durante a execução linear.

    `warnings` usa reducer operator.add — cada nó retorna apenas os
    avisos NOVOS que gerou; o LangGraph acumula automaticamente.
    """

    # ── Entrada ──────────────────────────────────────────────────────
    project_id: str
    dataset_filter: list[str]        # lista vazia = todos os datasets
    max_tables_per_dataset: int      # default 30

    # ── Saída do nó discover_datasets ────────────────────────────────
    datasets: list[dict[str, Any]]

    # ── Saída do nó discover_tables ──────────────────────────────────
    tables: list[dict[str, Any]]     # schema completo (colunas, tipos, modos)

    # ── Saída do nó infer_relationships ──────────────────────────────
    raw_relationships: list[dict[str, Any]]

    # ── Saída do nó enrich_with_llm ──────────────────────────────────
    relationships: list[dict[str, Any]]

    # ── Saída do nó build_graph_payload ──────────────────────────────
    graph_nodes: list[dict[str, Any]]   # {id, label, type, dataset, table_count, color}
    graph_edges: list[dict[str, Any]]   # {id, source, target, type, columns, strength, description, strategy}
    stats: dict[str, Any]               # métricas consolidadas

    # ── Controle ─────────────────────────────────────────────────────
    # Reducer: cada nó contribui apenas com seus novos avisos
    warnings: Annotated[list[str], operator.add]
    error: str | None
