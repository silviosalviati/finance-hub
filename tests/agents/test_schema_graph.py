"""Testes unitários para o agente SchemaGraphExplorer."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ─── Fixtures helpers ────────────────────────────────────────────────────────

def _make_table(full_name: str, dataset_id: str, columns: list[dict]) -> dict[str, Any]:
    parts = full_name.split(".")
    return {
        "full_name": full_name,
        "table_id": parts[-1],
        "dataset_id": dataset_id,
        "columns": columns,
        "partition_field": "",
        "clustering_fields": [],
    }


def _col(name: str, col_type: str = "INTEGER") -> dict[str, str]:
    return {"name": name, "type": col_type, "mode": "NULLABLE"}


# ─── Metadata ────────────────────────────────────────────────────────────────

def test_schema_graph_agent_metadata():
    from src.agents.schema_graph import SchemaGraphAgent
    agent = SchemaGraphAgent()
    assert agent.agent_id == "schema_graph"
    assert agent.display_name == "Schema Explorer"
    info = agent.runtime_info()
    assert info["agent_id"] == "schema_graph"
    assert "model" in info


# ─── infer_relationships ─────────────────────────────────────────────────────

def test_infer_relationships_explicit():
    """Colunas com sufixo _id presentes em múltiplas tabelas devem gerar relacionamento EXPLÍCITA."""
    from src.agents.schema_graph.nodes import infer_relationships

    tables = [
        _make_table("proj.ds.orders", "ds", [_col("customer_id"), _col("amount", "FLOAT")]),
        _make_table("proj.ds.customers", "ds", [_col("customer_id"), _col("name", "STRING")]),
    ]

    state = {"tables": tables, "warnings": []}
    result = infer_relationships(state)

    rels = result["raw_relationships"]
    assert len(rels) >= 1

    explicit = [r for r in rels if r["strategy"] == "EXPLICITA"]
    assert len(explicit) >= 1
    assert "customer_id" in explicit[0]["columns"]
    assert explicit[0]["strength"] >= 0.7


def test_infer_relationships_semantic():
    """Colunas com mesmo nome normalizado + tipo compatível entre tabelas distintas."""
    from src.agents.schema_graph.nodes import infer_relationships

    tables = [
        _make_table("proj.ds.invoices", "ds", [_col("status", "STRING"), _col("total", "FLOAT")]),
        _make_table("proj.ds.payments", "ds", [_col("status", "STRING"), _col("value", "FLOAT")]),
    ]

    state = {"tables": tables, "warnings": []}
    result = infer_relationships(state)

    rels = result["raw_relationships"]
    semantic = [r for r in rels if r["strategy"] == "SEMANTICA"]
    assert len(semantic) >= 1
    col_names = [c for r in semantic for c in r["columns"]]
    assert "status" in col_names


def test_infer_relationships_temporal():
    """Tabelas no mesmo dataset com colunas DATE devem gerar relacionamento TEMPORAL."""
    from src.agents.schema_graph.nodes import infer_relationships

    # Tabelas sem colunas em comum além de event_date, mas com nome diferente
    # para não acionar SEMANTICA no mesmo par com strength maior.
    tables = [
        _make_table("proj.ds.facts", "ds", [_col("transaction_date", "DATE"), _col("revenue", "FLOAT")]),
        _make_table("proj.ds.budget", "ds", [_col("reference_date", "DATE"), _col("allocated", "FLOAT")]),
    ]

    state = {"tables": tables, "warnings": []}
    result = infer_relationships(state)

    rels = result["raw_relationships"]
    temporal = [r for r in rels if r["strategy"] == "TEMPORAL"]
    assert len(temporal) >= 1
    assert temporal[0]["strength"] >= 0.35


# ─── build_graph_payload ─────────────────────────────────────────────────────

def test_build_graph_payload_structure():
    """Payload deve conter nós de tabela e coluna + arestas internas + arestas de relacionamento."""
    from src.agents.schema_graph.nodes import build_graph_payload

    datasets = [{"dataset_id": "ds", "full_name": "proj.ds", "description": "", "location": "US", "table_count": 2}]
    tables = [
        _make_table("proj.ds.orders", "ds", [_col("id"), _col("customer_id")]),
        _make_table("proj.ds.customers", "ds", [_col("customer_id"), _col("name", "STRING")]),
    ]
    relationships = [
        {
            "source_table": "proj.ds.orders",
            "target_table": "proj.ds.customers",
            "source_dataset": "ds",
            "target_dataset": "ds",
            "columns": ["customer_id"],
            "strategy": "EXPLICITA",
            "strength": 0.85,
            "rel_type": "FATO_DIMENSAO",
            "description": "Orders referencia customers via customer_id.",
        }
    ]

    state = {
        "datasets": datasets,
        "tables": tables,
        "relationships": relationships,
        "warnings": [],
    }

    result = build_graph_payload(state)

    assert result.get("error") is None
    nodes = result["graph_nodes"]
    edges = result["graph_edges"]

    node_ids = [n["id"] for n in nodes]

    # Dataset nodes removed; only table + column nodes expected
    assert "ds:ds" not in node_ids
    assert "tb:proj.ds.orders" in node_ids
    assert "tb:proj.ds.customers" in node_ids

    # Column nodes should exist
    assert "col:proj.ds.orders.customer_id" in node_ids
    assert "col:proj.ds.customers.customer_id" in node_ids

    # Column metadata
    cnode = next(n for n in nodes if n["id"] == "col:proj.ds.orders.customer_id")
    assert cnode["type"] == "column"
    assert cnode["parent_table"] == "tb:proj.ds.orders"
    assert cnode["is_key"] is True   # customer_id ends with _id

    # Edges: internal (table→col) + 1 relationship
    internal = [e for e in edges if e["type"] == "internal"]
    rel_edges = [e for e in edges if e["type"] != "internal"]
    assert len(internal) > 0
    assert len(rel_edges) == 1
    assert rel_edges[0]["type"] == "FATO_DIMENSAO"
    assert rel_edges[0]["strength"] == 0.85

    # Relationship edge should be column-level
    assert rel_edges[0]["source"].startswith("col:")
    assert rel_edges[0]["target"].startswith("col:")

    stats = result["stats"]
    assert stats["total_datasets"] == 1
    assert stats["total_tables"] == 2
    assert stats["total_relationships"] == 1


def test_no_tables_returns_error():
    """Estado sem tabelas deve retornar erro explícito no payload."""
    from src.agents.schema_graph.nodes import build_graph_payload

    state = {"datasets": [], "tables": [], "relationships": [], "warnings": []}
    result = build_graph_payload(state)
    assert result["error"] is not None
    assert result["graph_nodes"] == []
    assert result["graph_edges"] == []


def test_strength_ordering():
    """Relacionamentos devem ser ordenados por strength DESC após deduplicação."""
    from src.agents.schema_graph.nodes import _dedup_relationships

    rels = [
        {"source_table": "A", "target_table": "B", "columns": ["x"], "strength": 0.5, "rel_type": "EXPLICITA"},
        {"source_table": "A", "target_table": "B", "columns": ["x"], "strength": 0.8, "rel_type": "EXPLICITA"},
        {"source_table": "C", "target_table": "D", "columns": ["y"], "strength": 0.6, "rel_type": "SEMANTICA"},
    ]

    deduped = _dedup_relationships(rels)
    ab = next(r for r in deduped if r["source_table"] == "A" and r["target_table"] == "B")
    assert ab["strength"] == 0.8  # manteve o mais forte
    assert len(deduped) == 2
