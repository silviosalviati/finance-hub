from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.oauth2 import service_account

from src.api.dependencies import get_current_user
from src.core.checkpointer import CheckpointConfig, FileCheckpointer
from src.shared.config import GCP_CREDENTIALS_PATH, GCP_PROJECT_ID

router = APIRouter(tags=["schema-explorer"])

_cache = FileCheckpointer(
    CheckpointConfig(base_dir=Path(".sixth") / "schema_explorer_cache", ttl_hours=1)
)

# ── Column type groupings ────────────────────────────────────────────────────

_INT_TYPES = frozenset(
    {"INTEGER", "INT64", "INT", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT"}
)
_STR_TYPES = frozenset({"STRING", "VARCHAR", "CHAR", "BYTES"})
_NUM_TYPES = frozenset({"FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC", "DECIMAL"})
_DATE_TYPES = frozenset({"DATE", "DATETIME", "TIMESTAMP", "TIME"})


def _type_group(dtype: str) -> str:
    t = dtype.upper().split("(")[0].strip()
    if t in _INT_TYPES:
        return "int"
    if t in _STR_TYPES:
        return "str"
    if t in _NUM_TYPES:
        return "num"
    if t in _DATE_TYPES:
        return "date"
    return t


def _types_compatible(t1: str, t2: str) -> bool:
    g1, g2 = _type_group(t1), _type_group(t2)
    # int ↔ num are compatible for FK matching
    if {g1, g2} <= {"int", "num"}:
        return True
    return g1 == g2


# ── BigQuery client ──────────────────────────────────────────────────────────


def _get_bq_client(project_id: str) -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)
    return bigquery.Client(project=project_id, credentials=creds)


def _run_query_safe(client: bigquery.Client, sql: str) -> list[dict[str, Any]]:
    """Execute a query and return rows as dicts; returns [] on any error."""
    try:
        return [dict(row) for row in client.query(sql).result()]
    except Exception:
        return []


# ── Relationship inference ───────────────────────────────────────────────────


def _infer_relationships(
    tables: dict[str, list[dict[str, Any]]],
    fk_set: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    """Infer table relationships in confidence order.

    Priority:
      1. HIGH  — declared FOREIGN KEY in INFORMATION_SCHEMA
      2. MEDIUM — identical column name + compatible type in 2+ tables
      3. LOW   — column suffix _id/_fk/_key with prefix matching a table name
    """
    table_names = set(tables.keys())
    col_to_tables: dict[str, list[str]] = {}
    for tbl, cols in tables.items():
        for col in cols:
            col_to_tables.setdefault(col["name"], []).append(tbl)

    edges: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()

    # HIGH — FK declared
    for tbl, col_name in fk_set:
        if tbl not in tables:
            continue
        # BQ INFORMATION_SCHEMA does not expose the referenced table in
        # CONSTRAINT_COLUMN_USAGE alone, so we pair declared FK columns with
        # same-name columns in other tables (still highly reliable).
        for other in col_to_tables.get(col_name, []):
            if other == tbl:
                continue
            key = tuple(sorted([tbl, other])) + (col_name,)
            if key not in seen:
                seen.add(key)
                edges.append(
                    {
                        "source": tbl,
                        "target": other,
                        "via_column": col_name,
                        "confidence": "high",
                        "relationship_type": "one_to_many",
                    }
                )

    # MEDIUM — same column name, compatible types
    for col_name, tbls in col_to_tables.items():
        if len(tbls) < 2:
            continue
        for i, tbl_a in enumerate(tbls):
            col_a = next((c for c in tables.get(tbl_a, []) if c["name"] == col_name), None)
            for tbl_b in tbls[i + 1 :]:
                col_b = next(
                    (c for c in tables.get(tbl_b, []) if c["name"] == col_name), None
                )
                if col_a and col_b and _types_compatible(col_a["type"], col_b["type"]):
                    key = tuple(sorted([tbl_a, tbl_b])) + (col_name,)
                    if key not in seen:
                        seen.add(key)
                        edges.append(
                            {
                                "source": tbl_a,
                                "target": tbl_b,
                                "via_column": col_name,
                                "confidence": "medium",
                                "relationship_type": "one_to_many",
                            }
                        )

    # LOW — suffix heuristic
    for tbl, cols in tables.items():
        for col in cols:
            cname = col["name"]
            for suffix in ("_id", "_fk", "_key"):
                if cname.endswith(suffix):
                    prefix = cname[: -len(suffix)]
                    for candidate in (prefix, prefix + "s", prefix.rstrip("s") + "s"):
                        if candidate in table_names and candidate != tbl:
                            key = (tbl, candidate, cname)
                            if key not in seen:
                                seen.add(key)
                                edges.append(
                                    {
                                        "source": tbl,
                                        "target": candidate,
                                        "via_column": cname,
                                        "confidence": "low",
                                        "relationship_type": "one_to_many",
                                    }
                                )

    return edges


# ── Table type classification ────────────────────────────────────────────────


def _classify_table(name: str, columns: list[dict[str, Any]]) -> str:
    name_lower = name.lower()
    if name_lower.startswith(("stg_", "raw_", "staging_")):
        return "staging"
    if name_lower.startswith(("agg_", "mart_", "aggregated_")):
        return "aggregated"

    has_date = any(c["type"].upper().split("(")[0] in _DATE_TYPES for c in columns)
    numeric_cols = [
        c
        for c in columns
        if c["type"].upper().split("(")[0] in (_INT_TYPES | _NUM_TYPES)
    ]
    id_cols = [c for c in columns if c["name"].lower().endswith("_id")]
    string_cols = [
        c
        for c in columns
        if c["type"].upper().split("(")[0] in _STR_TYPES
        and not c["name"].lower().endswith("_id")
    ]

    if has_date and len(numeric_cols) >= 2 and len(id_cols) >= 2:
        return "fact"
    if id_cols and string_cols:
        return "dimension"
    return "unknown"


# ── Main endpoint ────────────────────────────────────────────────────────────


@router.get("/api/schema-explorer/graph")
async def get_schema_explorer_graph(
    project_id: str = Query(default=""),
    dataset_hint: str = Query(default=""),
    session: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    """Return ER diagram data (nodes + edges) for a BigQuery dataset."""
    resolved_project = (project_id or GCP_PROJECT_ID).strip()
    if not resolved_project:
        raise HTTPException(status_code=400, detail="project_id é obrigatório.")

    raw_dataset = dataset_hint.strip().strip("`")
    if not raw_dataset:
        raise HTTPException(status_code=400, detail="dataset_hint é obrigatório.")

    parts = raw_dataset.split(".")
    if len(parts) == 2:
        resolved_project, dataset_id = parts[0].strip(), parts[1].strip()
    elif len(parts) == 1:
        dataset_id = parts[0]
    else:
        raise HTTPException(
            status_code=400,
            detail="dataset_hint inválido. Use 'dataset' ou 'project.dataset'.",
        )

    cache_key = f"se_{resolved_project}_{dataset_id}"
    cached = _cache.load(cache_key)
    if cached is not None:
        return cached

    try:
        client = _get_bq_client(resolved_project)
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Erro ao conectar ao BigQuery: {exc}"
        ) from exc

    # ── Query 1: columns ──────────────────────────────────────────
    col_sql = f"""
    SELECT
      table_name,
      column_name,
      data_type,
      is_nullable,
      is_partitioning_column,
      ordinal_position
    FROM `{resolved_project}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    ORDER BY table_name, ordinal_position
    """

    # ── Query 2: declared constraints (PKs / FKs) ─────────────────
    constraint_sql = f"""
    SELECT
      ccu.constraint_name,
      ccu.table_name,
      ccu.column_name,
      tc.constraint_type
    FROM `{resolved_project}.{dataset_id}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE` AS ccu
    JOIN `{resolved_project}.{dataset_id}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` AS tc
      ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_name  = tc.table_name
    """

    # ── Query 3: clustering columns ───────────────────────────────
    clustering_sql = f"""
    SELECT table_name, column_name, clustering_ordinal_position
    FROM `{resolved_project}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE clustering_ordinal_position IS NOT NULL
    ORDER BY table_name, clustering_ordinal_position
    """

    col_rows = _run_query_safe(client, col_sql)
    if not col_rows:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Dataset '{resolved_project}.{dataset_id}' não encontrado "
                "ou não contém tabelas acessíveis."
            ),
        )

    constraint_rows = _run_query_safe(client, constraint_sql)
    clustering_rows = _run_query_safe(client, clustering_sql)

    # ── Build indexes ─────────────────────────────────────────────
    pk_set: set[tuple[str, str]] = set()
    fk_set: set[tuple[str, str]] = set()
    for r in constraint_rows:
        tbl = str(r.get("table_name") or "")
        col = str(r.get("column_name") or "")
        ctype = str(r.get("constraint_type") or "").upper()
        if ctype == "PRIMARY KEY":
            pk_set.add((tbl, col))
        elif ctype == "FOREIGN KEY":
            fk_set.add((tbl, col))

    clustering_index: dict[str, list[str]] = {}
    for r in clustering_rows:
        clustering_index.setdefault(str(r["table_name"]), []).append(
            str(r["column_name"])
        )

    partition_index: dict[str, str] = {}

    # ── Group columns by table ────────────────────────────────────
    tables_raw: dict[str, list[dict[str, Any]]] = {}
    for r in col_rows:
        tbl = str(r["table_name"])
        col_name = str(r["column_name"])
        dtype = str(r.get("data_type") or "STRING")
        is_part = str(r.get("is_partitioning_column") or "NO").upper() == "YES"

        if is_part:
            partition_index[tbl] = col_name

        is_pk = (tbl, col_name) in pk_set or col_name.lower() in (
            "id",
            f"{tbl.lower()}_id",
        )
        is_fk = (tbl, col_name) in fk_set

        tables_raw.setdefault(tbl, []).append(
            {
                "name": col_name,
                "type": dtype,
                "is_nullable": str(r.get("is_nullable") or "YES").upper() != "NO",
                "is_pk_candidate": is_pk,
                "is_fk_candidate": is_fk,
                "is_partition": is_part,
                "is_clustering": col_name in clustering_index.get(tbl, []),
            }
        )

    # ── Infer relationships ───────────────────────────────────────
    all_edges = _infer_relationships(tables_raw, fk_set)

    # Deduplicate: prefer higher confidence for same source/target/column triple
    conf_rank = {"high": 0, "medium": 1, "low": 2}
    best: dict[tuple[str, ...], dict[str, Any]] = {}
    for edge in all_edges:
        key = tuple(sorted([edge["source"], edge["target"]])) + (edge["via_column"],)
        if key not in best or conf_rank[edge["confidence"]] < conf_rank[best[key]["confidence"]]:
            best[key] = edge

    deduped_edges = list(best.values())

    # ── Build node list ───────────────────────────────────────────
    connected_tables = {e["source"] for e in deduped_edges} | {
        e["target"] for e in deduped_edges
    }
    isolated_tables = sorted(t for t in tables_raw if t not in connected_tables)

    nodes: list[dict[str, Any]] = []
    for tbl, cols in sorted(tables_raw.items()):
        nodes.append(
            {
                "id": tbl,
                "label": tbl,
                "table_type": _classify_table(tbl, cols),
                "row_count_estimate": None,
                "partition_field": partition_index.get(tbl),
                "clustering_fields": clustering_index.get(tbl, []),
                "columns": cols,
            }
        )

    result: dict[str, Any] = {
        "nodes": nodes,
        "edges": deduped_edges,
        "metadata": {
            "total_tables": len(nodes),
            "total_relationships": len(deduped_edges),
            "isolated_tables": isolated_tables,
            "dataset_ref": f"{resolved_project}.{dataset_id}",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    _cache.save(cache_key, result)
    return result
