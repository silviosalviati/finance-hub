"""Nós do grafo SchemaGraphExplorer.

Pipeline linear:
    discover_datasets → discover_tables → infer_relationships
        → enrich_with_llm → build_graph_payload
"""

from __future__ import annotations

import json
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.schema_graph.prompts import ENRICH_RELATIONSHIPS_PROMPT
from src.agents.schema_graph.state import SchemaGraphState
from src.shared.config import GCP_CREDENTIALS_PATH
from src.shared.tools.bigquery import get_dataset_tables_schema

# ─── Constantes ──────────────────────────────────────────────────────────────

_COLOR_DATASET = "#004691"   # porto-primary
_COLOR_TABLE = "#0891B2"     # teal

_COMPATIBLE_TYPES: dict[str, frozenset[str]] = {
    "INTEGER": frozenset({"INTEGER", "INT64", "INT", "NUMERIC", "BIGNUMERIC"}),
    "INT64": frozenset({"INTEGER", "INT64", "INT", "NUMERIC", "BIGNUMERIC"}),
    "INT": frozenset({"INTEGER", "INT64", "INT", "NUMERIC", "BIGNUMERIC"}),
    "NUMERIC": frozenset({"INTEGER", "INT64", "INT", "NUMERIC", "BIGNUMERIC", "FLOAT", "FLOAT64"}),
    "BIGNUMERIC": frozenset({"INTEGER", "INT64", "INT", "NUMERIC", "BIGNUMERIC", "FLOAT", "FLOAT64"}),
    "FLOAT": frozenset({"FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"}),
    "FLOAT64": frozenset({"FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"}),
    "STRING": frozenset({"STRING", "BYTES"}),
    "BYTES": frozenset({"STRING", "BYTES"}),
    "DATE": frozenset({"DATE", "DATETIME", "TIMESTAMP"}),
    "DATETIME": frozenset({"DATE", "DATETIME", "TIMESTAMP"}),
    "TIMESTAMP": frozenset({"DATE", "DATETIME", "TIMESTAMP"}),
    "BOOLEAN": frozenset({"BOOLEAN", "BOOL"}),
    "BOOL": frozenset({"BOOLEAN", "BOOL"}),
}

_DATE_TYPES: frozenset[str] = frozenset({
    "DATE", "DATETIME", "TIMESTAMP",
})

_ID_SUFFIXES: tuple[str, ...] = ("_id", "_fk", "_key", "_ref", "_code", "_cod")
_FK_PREFIXES: tuple[str, ...] = ("fk_", "id_")

_ENRICH_BATCH_SIZE = 30
_MAX_WORKERS = 4

# ─── Sanitização de entrada ──────────────────────────────────────────────────

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_\-]+$")


def _sanitize_identifier(value: str) -> str:
    """Remove caracteres inseguros de project/dataset IDs."""
    cleaned = value.strip()
    if not _SAFE_IDENTIFIER.match(cleaned):
        raise ValueError(f"Identificador inválido: {cleaned!r}")
    return cleaned


def _get_bq_client(project_id: str) -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(
        GCP_CREDENTIALS_PATH
    )
    return bigquery.Client(project=project_id, credentials=credentials)


def _normalize_col_name(name: str) -> str:
    """Normaliza nome de coluna: lower, remove acentos, colapsa underscores."""
    nfkd = unicodedata.normalize("NFKD", name.lower())
    ascii_only = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"_+", "_", ascii_only).strip("_")


def _types_compatible(type_a: str, type_b: str) -> bool:
    """Verifica compatibilidade de tipos para joins."""
    a = type_a.upper()
    b = type_b.upper()
    if a == b:
        return True
    compat = _COMPATIBLE_TYPES.get(a)
    return b in compat if compat else False


def _is_id_column(col_name: str) -> bool:
    name_lower = col_name.lower()
    if any(name_lower.endswith(s) for s in _ID_SUFFIXES):
        return True
    if any(name_lower.startswith(p) for p in _FK_PREFIXES):
        return True
    return False


def _is_date_column(col_type: str) -> bool:
    return col_type.upper() in _DATE_TYPES


# ─── Nó 1: discover_datasets ─────────────────────────────────────────────────

def discover_datasets(state: SchemaGraphState) -> dict[str, Any]:
    """Lista todos os datasets do projeto via BQ API.

    Respeita `dataset_filter` se informado.
    Registra warnings para datasets inacessíveis e continua.
    """
    project_id = (state.get("project_id") or "").strip()
    if not project_id:
        return {"error": "project_id nao informado.", "datasets": [], "warnings": []}

    try:
        project_id = _sanitize_identifier(project_id)
    except ValueError as exc:
        return {"error": str(exc), "datasets": [], "warnings": []}

    dataset_filter: list[str] = [
        f.strip() for f in (state.get("dataset_filter") or []) if f.strip()
    ]

    warnings: list[str] = list(state.get("warnings") or [])
    datasets: list[dict[str, Any]] = []

    try:
        client = _get_bq_client(project_id)
        bq_datasets = list(client.list_datasets(project=project_id))
    except Exception as exc:
        return {
            "error": f"Falha ao listar datasets do projeto {project_id}: {exc}",
            "datasets": [],
            "warnings": warnings,
        }

    for ds_item in bq_datasets:
        ds_id = ds_item.dataset_id
        if dataset_filter and ds_id not in dataset_filter:
            continue

        try:
            ds_ref = client.get_dataset(f"{project_id}.{ds_id}")
            table_count = sum(1 for _ in client.list_tables(f"{project_id}.{ds_id}"))
            datasets.append({
                "dataset_id": ds_id,
                "full_name": f"{project_id}.{ds_id}",
                "location": ds_ref.location or "",
                "description": ds_ref.description or "",
                "table_count": table_count,
            })
        except Exception as exc:
            warnings.append(f"Dataset {ds_id} ignorado: {exc}")

    if not datasets:
        filter_msg = f" (filtro: {dataset_filter})" if dataset_filter else ""
        return {
            "error": f"Nenhum dataset encontrado no projeto {project_id}{filter_msg}.",
            "datasets": [],
            "warnings": warnings,
        }

    return {"datasets": datasets, "warnings": warnings, "error": None}


# ─── Nó 2: discover_tables ───────────────────────────────────────────────────

def _fetch_dataset_schema(
    project_id: str,
    dataset_id: str,
    max_tables: int,
    warnings_acc: list[str],
) -> list[dict[str, Any]]:
    """Busca schema completo de um único dataset (executado em thread pool)."""
    try:
        result = get_dataset_tables_schema(
            project_id=project_id,
            dataset_hint=dataset_id,
            max_tables=max_tables,
            max_columns=100,
        )
        tables = result.get("tables", [])
        if len(tables) >= max_tables:
            warnings_acc.append(
                f"Dataset {dataset_id}: limite de {max_tables} tabelas atingido. "
                "Algumas tabelas podem ter sido omitidas."
            )
        for tbl in tables:
            tbl["dataset_id"] = dataset_id
        return tables
    except Exception as exc:
        warnings_acc.append(f"Dataset {dataset_id} — erro ao listar tabelas: {exc}")
        return []


def discover_tables(state: SchemaGraphState) -> dict[str, Any]:
    """Para cada dataset descoberto, busca tabelas e schemas com paralelismo."""
    datasets: list[dict[str, Any]] = state.get("datasets") or []
    if not datasets:
        return {"error": "Nenhum dataset disponível para introspecção.", "tables": []}

    project_id = (state.get("project_id") or "").strip()
    max_tables = int(state.get("max_tables_per_dataset") or 30)
    max_tables = min(max(1, max_tables), 100)

    warnings: list[str] = list(state.get("warnings") or [])
    all_tables: list[dict[str, Any]] = []

    dataset_ids = [ds["dataset_id"] for ds in datasets]

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _fetch_dataset_schema, project_id, ds_id, max_tables, warnings
            ): ds_id
            for ds_id in dataset_ids
        }
        for future in as_completed(futures):
            tables = future.result()
            all_tables.extend(tables)

    if not all_tables:
        return {
            "error": "Nenhuma tabela encontrada nos datasets do projeto.",
            "tables": [],
            "warnings": warnings,
        }

    return {"tables": all_tables, "warnings": warnings, "error": None}


# ─── Nó 3: infer_relationships ───────────────────────────────────────────────

def _build_column_index(
    tables: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Indexa colunas por nome normalizado para lookup rápido."""
    index: dict[str, list[dict[str, Any]]] = {}
    for tbl in tables:
        for col in tbl.get("columns", []):
            norm = _normalize_col_name(col["name"])
            entry = {
                "table_full_name": tbl["full_name"],
                "dataset_id": tbl.get("dataset_id", ""),
                "col_name": col["name"],
                "col_type": col.get("type", "STRING"),
                "col_mode": col.get("mode", "NULLABLE"),
            }
            index.setdefault(norm, []).append(entry)
    return index


def _dedup_relationships(
    relationships: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Mantém, por par de tabelas, o relacionamento com maior strength."""
    best: dict[tuple[str, str, str], dict[str, Any]] = {}
    for rel in relationships:
        key: tuple[str, str, str] = (
            rel["source_table"],
            rel["target_table"],
            ",".join(sorted(rel.get("columns", []))),
        )
        existing = best.get(key)
        if existing is None or rel["strength"] > existing["strength"]:
            best[key] = rel
    return list(best.values())


def infer_relationships(state: SchemaGraphState) -> dict[str, Any]:
    """Infere relacionamentos entre tabelas usando três estratégias puras.

    Estratégias:
      - EXPLICITA: colunas com sufixo/prefixo de FK em múltiplas tabelas
      - SEMANTICA: mesmo nome normalizado + tipo compatível entre tabelas distintas
      - TEMPORAL: colunas de partição/data entre tabelas do mesmo dataset
    """
    tables: list[dict[str, Any]] = state.get("tables") or []
    if not tables:
        return {"raw_relationships": [], "warnings": state.get("warnings") or []}

    warnings: list[str] = list(state.get("warnings") or [])
    raw: list[dict[str, Any]] = []

    col_index = _build_column_index(tables)

    # ── Estratégia EXPLÍCITA ──────────────────────────────────────────────
    for norm_name, entries in col_index.items():
        if len(entries) < 2:
            continue
        orig_name = entries[0]["col_name"]
        if not _is_id_column(orig_name):
            continue

        for i, src in enumerate(entries):
            for tgt in entries[i + 1:]:
                if src["table_full_name"] == tgt["table_full_name"]:
                    continue
                if not _types_compatible(src["col_type"], tgt["col_type"]):
                    continue

                strength = 0.7
                if norm_name.endswith("_id") or norm_name.endswith("_fk"):
                    strength += 0.15
                if src["col_type"].upper() == tgt["col_type"].upper():
                    strength += 0.10
                strength = round(min(1.0, strength), 4)

                raw.append({
                    "source_table": src["table_full_name"],
                    "target_table": tgt["table_full_name"],
                    "source_dataset": src["dataset_id"],
                    "target_dataset": tgt["dataset_id"],
                    "columns": [orig_name],
                    "strategy": "EXPLICITA",
                    "strength": strength,
                    "rel_type": "CHAVE_ESTRANGEIRA",
                    "description": "",
                })

    # ── Estratégia SEMÂNTICA ──────────────────────────────────────────────
    for norm_name, entries in col_index.items():
        if len(entries) < 2:
            continue
        if _is_id_column(entries[0]["col_name"]):
            continue  # já tratado na estratégia explícita

        for i, src in enumerate(entries):
            for tgt in entries[i + 1:]:
                if src["table_full_name"] == tgt["table_full_name"]:
                    continue
                if not _types_compatible(src["col_type"], tgt["col_type"]):
                    continue

                # força mais baixa para semântica (inferência menos certa)
                strength = 0.4
                if src["col_type"].upper() == tgt["col_type"].upper():
                    strength += 0.10
                # bônus: mesma frequência de ocorrência no mesmo dataset
                if src["dataset_id"] == tgt["dataset_id"]:
                    strength += 0.05
                strength = round(min(1.0, strength), 4)

                raw.append({
                    "source_table": src["table_full_name"],
                    "target_table": tgt["table_full_name"],
                    "source_dataset": src["dataset_id"],
                    "target_dataset": tgt["dataset_id"],
                    "columns": [src["col_name"]],
                    "strategy": "SEMANTICA",
                    "strength": strength,
                    "rel_type": "COMPARTILHADA",
                    "description": "",
                })

    # ── Estratégia TEMPORAL ───────────────────────────────────────────────
    # Agrupa tabelas por dataset; se duas tabelas no mesmo dataset têm colunas
    # de data/partição, inferimos possível join temporal.
    tables_by_dataset: dict[str, list[dict[str, Any]]] = {}
    for tbl in tables:
        ds = tbl.get("dataset_id", "")
        tables_by_dataset.setdefault(ds, []).append(tbl)

    for ds_id, ds_tables in tables_by_dataset.items():
        date_tables: list[tuple[dict[str, Any], str]] = []
        for tbl in ds_tables:
            partition_field = tbl.get("partition_field", "") or ""
            if partition_field:
                date_tables.append((tbl, partition_field))
                continue
            for col in tbl.get("columns", []):
                if _is_date_column(col.get("type", "")):
                    date_tables.append((tbl, col["name"]))
                    break

        for i, (src_tbl, src_date_col) in enumerate(date_tables):
            for tgt_tbl, tgt_date_col in date_tables[i + 1:]:
                if src_tbl["full_name"] == tgt_tbl["full_name"]:
                    continue

                strength = 0.35
                if src_tbl.get("partition_field") and tgt_tbl.get("partition_field"):
                    strength = 0.55  # ambas particionadas: join mais confiável
                elif src_date_col == tgt_date_col:
                    strength = 0.45  # mesmo nome de coluna de data
                strength = round(min(1.0, strength), 4)

                raw.append({
                    "source_table": src_tbl["full_name"],
                    "target_table": tgt_tbl["full_name"],
                    "source_dataset": ds_id,
                    "target_dataset": ds_id,
                    "columns": sorted({src_date_col, tgt_date_col}),
                    "strategy": "TEMPORAL",
                    "strength": strength,
                    "rel_type": "TEMPORAL",
                    "description": "",
                })

    deduped = _dedup_relationships(raw)
    deduped.sort(key=lambda r: r["strength"], reverse=True)

    return {
        "raw_relationships": deduped,
        "warnings": warnings,
        "error": None,
    }


# ─── Nó 4: enrich_with_llm ───────────────────────────────────────────────────

_FALLBACK_TYPES: dict[str, str] = {
    "CHAVE_ESTRANGEIRA": "FATO_DIMENSAO",
    "COMPARTILHADA": "DIMENSAO_DIMENSAO",
    "TEMPORAL": "TEMPORAL",
}


def _fallback_enrich(rel: dict[str, Any]) -> dict[str, Any]:
    """Enriquecimento determinístico quando o LLM falha."""
    rel_type = _FALLBACK_TYPES.get(rel.get("rel_type", ""), "COMPARTILHADA")
    cols = ", ".join(rel.get("columns", []))
    description = (
        f"Relacionamento inferido via estratégia {rel.get('strategy', '?')} "
        f"através da(s) coluna(s): {cols}."
    )
    return {**rel, "rel_type": rel_type, "description": description}


def _parse_enrich_response(text: str, originals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extrai lista de relacionamentos enriquecidos da resposta do LLM."""
    raw = text.strip()
    # Remove possíveis blocos de código markdown
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            raise ValueError("Resposta nao e uma lista JSON")

        enriched = []
        for i, orig in enumerate(originals):
            if i < len(parsed) and isinstance(parsed[i], dict):
                item = parsed[i]
                enriched.append({
                    **orig,
                    "rel_type": str(item.get("rel_type") or orig.get("rel_type", "")),
                    "description": str(item.get("description") or ""),
                })
            else:
                enriched.append(_fallback_enrich(orig))
        return enriched
    except Exception:
        return [_fallback_enrich(r) for r in originals]


def enrich_with_llm(state: SchemaGraphState, llm: BaseChatModel) -> dict[str, Any]:
    """Envia relacionamentos brutos ao LLM para categorização e descrição.

    Processa em batches de no máximo _ENRICH_BATCH_SIZE relacionamentos.
    Usa enriquecimento determinístico como fallback em caso de falha do LLM.
    """
    raw: list[dict[str, Any]] = state.get("raw_relationships") or []
    if not raw:
        return {"relationships": [], "error": None}

    warnings: list[str] = list(state.get("warnings") or [])
    enriched_all: list[dict[str, Any]] = []

    batches = [
        raw[i: i + _ENRICH_BATCH_SIZE]
        for i in range(0, len(raw), _ENRICH_BATCH_SIZE)
    ]

    for batch_idx, batch in enumerate(batches):
        batch_summary = [
            {
                "source": r["source_table"],
                "target": r["target_table"],
                "columns": r.get("columns", []),
                "strategy": r.get("strategy", ""),
                "current_type": r.get("rel_type", ""),
            }
            for r in batch
        ]

        prompt_content = (
            f"{ENRICH_RELATIONSHIPS_PROMPT}\n\n"
            f"RELACIONAMENTOS PARA ENRIQUECER:\n"
            f"{json.dumps(batch_summary, ensure_ascii=False, indent=2)}"
        )

        try:
            response = llm.invoke(
                [
                    SystemMessage(content=ENRICH_RELATIONSHIPS_PROMPT),
                    HumanMessage(
                        content=(
                            f"Enriqueça os seguintes {len(batch)} relacionamentos. "
                            f"Responda APENAS JSON válido:\n\n"
                            f"{json.dumps(batch_summary, ensure_ascii=False, indent=2)}"
                        )
                    ),
                ]
            )
            text = str(response.content) if hasattr(response, "content") else str(response)
            enriched_batch = _parse_enrich_response(text, batch)
        except Exception as exc:
            warnings.append(
                f"Enriquecimento LLM falhou no batch {batch_idx + 1}: {exc}. "
                "Usando categorização determinística."
            )
            enriched_batch = [_fallback_enrich(r) for r in batch]

        enriched_all.extend(enriched_batch)

    enriched_all.sort(key=lambda r: r["strength"], reverse=True)

    return {"relationships": enriched_all, "warnings": warnings, "error": None}


# ─── Nó 5: build_graph_payload ───────────────────────────────────────────────

_REL_TYPE_COLORS: dict[str, str] = {
    "FATO_DIMENSAO": "#059669",
    "DIMENSAO_DIMENSAO": "#d97706",
    "HIERARQUICO": "#6d28d9",
    "TEMPORAL": "#0891b2",
    "CHAVE_ESTRANGEIRA": "#059669",
    "COMPARTILHADA": "#d97706",
}

_EDGE_ID_COUNTER: int = 0


def _edge_id(src: str, tgt: str) -> str:
    safe_src = re.sub(r"[^a-zA-Z0-9]", "_", src)
    safe_tgt = re.sub(r"[^a-zA-Z0-9]", "_", tgt)
    return f"edge_{safe_src}__{safe_tgt}"


def build_graph_payload(state: SchemaGraphState) -> dict[str, Any]:
    """Monta graph_nodes, graph_edges e stats para o frontend.

    Nós:
      - dataset: {id: "ds:{dataset_id}", label, type:"dataset", color}
      - table:   {id: "tb:{full_name}", label, type:"table", dataset, row_count, column_count, color}

    Arestas ordenadas por strength DESC.
    """
    datasets: list[dict[str, Any]] = state.get("datasets") or []
    tables: list[dict[str, Any]] = state.get("tables") or []
    relationships: list[dict[str, Any]] = state.get("relationships") or []
    warnings: list[str] = list(state.get("warnings") or [])

    if not tables:
        return {
            "error": "Nenhuma tabela descoberta. Verifique o projeto e os datasets.",
            "graph_nodes": [],
            "graph_edges": [],
            "stats": {},
            "warnings": warnings,
        }

    # ── Nós de dataset ────────────────────────────────────────────────────
    graph_nodes: list[dict[str, Any]] = []
    dataset_table_counts: dict[str, int] = {}

    for tbl in tables:
        ds_id = tbl.get("dataset_id", "")
        dataset_table_counts[ds_id] = dataset_table_counts.get(ds_id, 0) + 1

    for ds in datasets:
        ds_id = ds["dataset_id"]
        graph_nodes.append({
            "id": f"ds:{ds_id}",
            "label": ds_id,
            "type": "dataset",
            "dataset": ds_id,
            "table_count": dataset_table_counts.get(ds_id, 0),
            "description": ds.get("description", ""),
            "color": _COLOR_DATASET,
        })

    # ── Nós de tabela ─────────────────────────────────────────────────────
    for tbl in tables:
        full_name = tbl.get("full_name", "")
        label = tbl.get("table_id", full_name.split(".")[-1] if "." in full_name else full_name)
        graph_nodes.append({
            "id": f"tb:{full_name}",
            "label": label,
            "type": "table",
            "dataset": tbl.get("dataset_id", ""),
            "full_name": full_name,
            "column_count": len(tbl.get("columns", [])),
            "partition_field": tbl.get("partition_field", ""),
            "clustering_fields": tbl.get("clustering_fields", []),
            "columns": tbl.get("columns", []),
            "color": _COLOR_TABLE,
        })

    # ── Arestas ───────────────────────────────────────────────────────────
    graph_edges: list[dict[str, Any]] = []
    seen_edges: set[str] = set()

    for rel in sorted(relationships, key=lambda r: r["strength"], reverse=True):
        src_id = f"tb:{rel['source_table']}"
        tgt_id = f"tb:{rel['target_table']}"
        edge_key = f"{src_id}||{tgt_id}"
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)

        rel_type = rel.get("rel_type", "COMPARTILHADA")
        graph_edges.append({
            "id": _edge_id(rel["source_table"], rel["target_table"]),
            "source": src_id,
            "target": tgt_id,
            "type": rel_type,
            "columns": rel.get("columns", []),
            "strength": rel.get("strength", 0.0),
            "description": rel.get("description", ""),
            "strategy": rel.get("strategy", ""),
            "color": _REL_TYPE_COLORS.get(rel_type, "#8096b2"),
        })

    # ── Stats ─────────────────────────────────────────────────────────────
    total_ds = len(datasets)
    total_tb = len(tables)
    total_edges = len(graph_edges)
    density = round(total_edges / max(total_tb * (total_tb - 1) / 2, 1), 4) if total_tb > 1 else 0.0

    # Top 5 tabelas mais conectadas (degree)
    degree: dict[str, int] = {}
    for edge in graph_edges:
        degree[edge["source"]] = degree.get(edge["source"], 0) + 1
        degree[edge["target"]] = degree.get(edge["target"], 0) + 1

    top_connected = sorted(degree.items(), key=lambda x: x[1], reverse=True)[:5]

    rel_type_dist: dict[str, int] = {}
    for edge in graph_edges:
        t = edge["type"]
        rel_type_dist[t] = rel_type_dist.get(t, 0) + 1

    stats: dict[str, Any] = {
        "total_datasets": total_ds,
        "total_tables": total_tb,
        "total_relationships": total_edges,
        "graph_density": density,
        "top_connected_tables": [
            {"node_id": nid, "degree": deg} for nid, deg in top_connected
        ],
        "relationship_type_distribution": rel_type_dist,
    }

    return {
        "graph_nodes": graph_nodes,
        "graph_edges": graph_edges,
        "stats": stats,
        "warnings": warnings,
        "error": None,
    }
