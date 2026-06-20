"""RAG sobre o catálogo do BigQuery — datasets/tabelas/colunas por significado.

O Planner hoje só descobre dataset/tabela por nome (`bq_list_datasets` +
fuzzy match) ou chutando um nome plausível — falha sempre que o nome do
dataset não tem relação textual com o que ele contém (ex.: um dataset
`logistica_vendas` que na verdade guarda dados de "contas a receber").

Este módulo indexa o catálogo real — usando as descrições de coluna que já
existem no BigQuery — em embeddings, e permite buscar as tabelas certas pelo
SIGNIFICADO da pergunta do usuário, independente do nome do dataset/tabela.

Dado o tamanho real do catálogo (poucos datasets, dezenas de tabelas), uma
varredura em memória com similaridade de cosseno é suficiente — sem
necessidade de um vector DB dedicado.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from src.core.database import (
    get_catalog_oldest_update,
    list_catalog_entries,
    upsert_catalog_entry,
)
from src.shared.config import get_runtime_config
from src.shared.tools.bigquery import get_dataset_tables_schema, list_datasets_with_labels

_DEFAULT_TTL_HOURS = 24
_DEFAULT_EMBEDDING_MODEL = "text-embedding-005"

_embeddings_singleton: Any = None


def _get_embeddings() -> Any:
    """Cliente de embeddings — Vertex AI, mesmas credenciais já configuradas.

    `VertexAIEmbeddings` está deprecado em favor de `GoogleGenerativeAIEmbeddings`,
    mas este projeto autentica via service account/ADC (Vertex AI), não via
    API key da Gemini Developer API — migrar exigiria um modelo de
    autenticação novo. Mantemos `VertexAIEmbeddings` deliberadamente.
    """
    global _embeddings_singleton
    if _embeddings_singleton is None:
        from langchain_google_vertexai import VertexAIEmbeddings

        from src.shared.tools.llm import _ensure_google_adc_env

        _ensure_google_adc_env()
        _embeddings_singleton = VertexAIEmbeddings(
            model_name=get_runtime_config("FINANCE_AUDITOR_EMBEDDING_MODEL", _DEFAULT_EMBEDDING_MODEL),
            project=get_runtime_config("VERTEXAI_PROJECT", "silviosalviati"),
            location=get_runtime_config("VERTEXAI_LOCATION", "us-central1"),
        )
    return _embeddings_singleton


def _table_summary(dataset_id: str, table: dict[str, Any]) -> str:
    cols = table.get("columns") or []
    parts: list[str] = []
    for col in cols[:50]:
        name = str(col.get("name") or "")
        if not name:
            continue
        desc = str(col.get("description") or "").strip()
        parts.append(f"{name} ({desc})" if desc else name)
    table_id = table.get("table_id", "")
    return f"{dataset_id}.{table_id}: colunas — " + ", ".join(parts)


def _is_stale(oldest_iso: str | None, ttl_hours: int) -> bool:
    if not oldest_iso:
        return True
    try:
        oldest = datetime.fromisoformat(oldest_iso)
    except ValueError:
        return True
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - oldest > timedelta(hours=ttl_hours)


def reindex_catalog(project_id: str, force: bool = False) -> dict[str, Any]:
    """Reindexa o catálogo (datasets/tabelas/colunas) de um projeto.

    Sem `force`, só reindexa quando o índice estiver mais velho que o TTL
    configurado (`FINANCE_AUDITOR_CATALOG_TTL_HOURS`, default 24h) — mesmo
    espírito do cache de catálogo de gerência já usado em `agents.py`.
    """
    ttl_hours = int(
        get_runtime_config("FINANCE_AUDITOR_CATALOG_TTL_HOURS", str(_DEFAULT_TTL_HOURS))
    )
    if not force and not _is_stale(get_catalog_oldest_update(project_id), ttl_hours):
        return {"reindexed": False, "reason": "índice ainda dentro do TTL"}

    try:
        datasets = list_datasets_with_labels(project_id)
    except Exception as exc:  # noqa: BLE001
        return {"reindexed": False, "reason": f"falha ao listar datasets: {exc}"}

    embeddings = _get_embeddings()
    indexed = 0
    for ds in datasets:
        dataset_id = ds["dataset_id"]
        try:
            info = get_dataset_tables_schema(project_id, dataset_id, max_tables=80, max_columns=50)
        except Exception:  # noqa: BLE001
            continue
        tables = info.get("tables") or []
        if not tables:
            continue
        summaries = [_table_summary(dataset_id, t) for t in tables]
        try:
            vectors = embeddings.embed_documents(summaries)
        except Exception:  # noqa: BLE001
            continue
        for table, summary, vector in zip(tables, summaries, vectors):
            table_id = table.get("table_id", "")
            full_name = table.get("full_name") or f"{project_id}.{dataset_id}.{table_id}"
            upsert_catalog_entry(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                full_name=full_name,
                text_summary=summary,
                embedding_json=json.dumps(vector),
            )
            indexed += 1

    return {"reindexed": True, "tables_indexed": indexed, "datasets": len(datasets)}


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if not norm_a or not norm_b:
        return 0.0
    return dot / (norm_a * norm_b)


def search_catalog(project_id: str, query: str, top_k: int = 5) -> list[dict[str, Any]]:
    """Busca as tabelas mais relevantes para `query` por significado.

    Reindexa automaticamente (respeitando o TTL) antes de buscar, para que o
    índice nunca fique vazio/desatualizado sem o caller precisar saber disso.

    Tolera ambientes sem schema/tabela ``finance_catalog_index`` inicializada
    (ex.: testes unitários que não chamam ``init_db()``) ou sem credenciais
    de embedding: devolve lista vazia em vez de propagar a exceção.
    """
    query = (query or "").strip()
    if not query:
        return []

    try:
        reindex_catalog(project_id, force=False)
        entries = list_catalog_entries(project_id)
    except Exception:  # noqa: BLE001 — fallback gracioso para qualquer falha de I/O
        return []
    if not entries:
        return []

    embeddings = _get_embeddings()
    try:
        query_vector = embeddings.embed_query(query)
    except Exception:  # noqa: BLE001
        return []

    scored: list[tuple[float, dict[str, Any]]] = []
    for entry in entries:
        try:
            vector = json.loads(entry.get("embedding_json") or "[]")
        except (TypeError, ValueError):
            continue
        if not vector:
            continue
        score = _cosine_similarity(query_vector, vector)
        scored.append((score, entry))

    scored.sort(key=lambda item: item[0], reverse=True)
    limit = max(1, min(int(top_k or 5), 20))
    return [
        {
            "dataset_id": e["dataset_id"],
            "table_id": e["table_id"],
            "full_name": e["full_name"],
            "text_summary": e["text_summary"],
            "score": round(score, 4),
        }
        for score, e in scored[:limit]
    ]


__all__ = ["reindex_catalog", "search_catalog"]
