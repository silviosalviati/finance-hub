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

import asyncio
import json
import logging
import math
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any

from src.core.database import (
    get_catalog_oldest_update,
    list_catalog_entries,
    upsert_catalog_entry,
    upsert_finance_metric,
)
from src.shared.config import get_runtime_config, get_vertexai_project
from src.shared.tools.bigquery import (
    execute_query_rows,
    get_dataset_tables_schema,
    list_datasets_with_labels,
    list_table_ids,
)

_logger = logging.getLogger(__name__)

_DEFAULT_TTL_HOURS = 24
_DEFAULT_EMBEDDING_MODEL = "text-embedding-005"

# Convenção de nome fixa do Gold Layer (igual em qualquer gerência) — o que
# NÃO é fixo é em qual dataset ela mora, por isso a varredura abaixo passa
# por todos os datasets do projeto em vez de assumir um dataset específico.
_GOLD_METRIC_CATALOG_TABLE = "GOLD_METRIC_CATALOG"

_embeddings_singleton: Any = None


def _get_embeddings() -> Any:
    """Cliente de embeddings — Vertex AI, mesmas credenciais já configuradas.

    Desde `langchain-google-genai` 4.0.0, `GoogleGenerativeAIEmbeddings` passou
    a suportar o backend Vertex AI (via ADC) além da Gemini Developer API —
    `vertexai=True` + `project` usa as mesmas credenciais de service account
    já configuradas, sem precisar de API key. Substitui a antiga
    `VertexAIEmbeddings` (deprecada), mantendo o mesmo modelo e, portanto, a
    mesma dimensionalidade dos embeddings já persistidos no catálogo.
    """
    global _embeddings_singleton
    if _embeddings_singleton is None:
        from langchain_google_genai import GoogleGenerativeAIEmbeddings

        from src.shared.tools.llm import _ensure_google_adc_env

        _ensure_google_adc_env()
        _embeddings_singleton = GoogleGenerativeAIEmbeddings(
            model=get_runtime_config("FINANCE_AUDITOR_EMBEDDING_MODEL", _DEFAULT_EMBEDDING_MODEL),
            project=get_vertexai_project(),
            location=get_runtime_config("VERTEXAI_LOCATION", "us-central1"),
            vertexai=True,
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


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text or "")
        if unicodedata.category(c) != "Mn"
    )


def _slug(text: str) -> str:
    base = _strip_accents(text).lower()
    base = re.sub(r"[^a-z0-9]+", "_", base)
    return base.strip("_")


def sync_gold_metric_catalog(project_id: str) -> dict[str, Any]:
    """Sincroniza `GOLD_METRIC_CATALOG` (Gold Layer) para `finance_semantic_metrics`.

    Cada gerência tem o seu próprio dataset gold — não existe UM dataset/
    tabela fixo para todo o projeto. Por isso a varredura passa por TODOS os
    datasets do projeto e sincroniza onde encontrar uma tabela chamada
    `GOLD_METRIC_CATALOG` (convenção de nome, não de localização); datasets
    sem essa tabela são ignorados em silêncio — nem toda gerência precisa
    ter métricas oficiais cadastradas.

    A chave em `finance_semantic_metrics` é namespaced por
    `{project}.{dataset}.{metric}` para que duas gerências possam ter uma
    métrica com o mesmo nome (ex.: "TAXA_INADIMPLENCIA") sem colidir.
    """
    try:
        datasets = list_datasets_with_labels(project_id)
    except Exception as exc:  # noqa: BLE001
        return {"datasets_scanned": 0, "datasets_with_catalog": 0, "synced": 0, "errors": [str(exc)]}

    synced = 0
    with_catalog = 0
    errors: list[str] = []

    for ds in datasets:
        dataset_id = ds["dataset_id"]
        try:
            table_ids = list_table_ids(project_id, dataset_id)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{dataset_id}: falha ao listar tabelas ({exc})")
            continue
        if _GOLD_METRIC_CATALOG_TABLE not in table_ids:
            continue

        with_catalog += 1
        table_ref = f"{project_id}.{dataset_id}.{_GOLD_METRIC_CATALOG_TABLE}"
        try:
            rows, _ = execute_query_rows(f"SELECT * FROM `{table_ref}`", project_id, max_rows=500)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{table_ref}: falha ao ler ({exc})")
            continue

        for row in rows:
            metric_name = str(row.get("METRIC_NAME") or "").strip()
            if not metric_name:
                continue
            metric_id = str(row.get("METRIC_ID") or "").strip()
            key = f"{project_id}.{dataset_id}.{_slug(metric_id or metric_name)}"

            source_table_raw = str(row.get("SOURCE_TABLE") or "").strip()
            source_table = f"{project_id}.{dataset_id}.{source_table_raw}" if source_table_raw else ""
            # SQL_TEMPLATE é mais preciso (CASE/condições); FORMULA_SQL é o
            # resumo simples usado quando a métrica não precisa de CASE.
            sql_template = (
                str(row.get("SQL_TEMPLATE") or "").strip()
                or str(row.get("FORMULA_SQL") or "").strip()
            )

            try:
                upsert_finance_metric(
                    key,
                    name=metric_name,
                    description=str(row.get("DESCRICAO") or ""),
                    source_table=source_table,
                    sql_template=sql_template,
                    owner=str(row.get("OWNER") or ""),
                    tags=str(row.get("NIVEL") or ""),
                    domain=str(row.get("DOMINIO") or "").strip().lower(),
                    is_official=bool(row.get("OFICIAL")),
                )
                synced += 1
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{key}: falha ao sincronizar ({exc})")

    return {
        "datasets_scanned": len(datasets),
        "datasets_with_catalog": with_catalog,
        "synced": synced,
        "errors": errors,
    }


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


async def warmup_catalog_loop(project_ids: list[str]) -> None:
    """Mantém o índice do catálogo sempre quente, fora do request do usuário.

    Sem isso, a primeira busca depois do TTL vencer paga o custo total da
    reindexação (listar datasets, schema de cada um, embeddings) dentro da
    resposta de algum usuário. Aqui a checagem (e reindexação, se estiver
    vencida) roda em loop de fundo, bem antes do TTL vencer de fato.

    Pensado para ser disparado via `asyncio.create_task` no lifespan do app
    e cancelado no shutdown — nunca levanta, só loga e segue.
    """
    ttl_hours = int(
        get_runtime_config("FINANCE_AUDITOR_CATALOG_TTL_HOURS", str(_DEFAULT_TTL_HOURS))
    )
    # Confere bem antes do TTL vencer (margem de 1h), nunca depois. Para TTLs
    # bem curtos (<=1h, ex.: testes manuais), confere na metade do prazo.
    interval_hours = (ttl_hours - 1) if ttl_hours > 1 else max(ttl_hours / 2, 0.05)

    while True:
        for project_id in project_ids:
            try:
                result = await asyncio.to_thread(reindex_catalog, project_id, False)
                if result.get("reindexed"):
                    _logger.info(
                        "Catálogo pré-aquecido: %s (%s tabelas)",
                        project_id, result.get("tables_indexed"),
                    )
            except Exception:  # noqa: BLE001 — warmup nunca deve derrubar o processo
                _logger.exception("Falha ao pré-aquecer catálogo de %s", project_id)
            try:
                metric_result = await asyncio.to_thread(sync_gold_metric_catalog, project_id)
                if metric_result.get("synced"):
                    _logger.info(
                        "Gold Metric Catalog sincronizado: %s (%s métricas, %s dataset(s) com catálogo)",
                        project_id, metric_result.get("synced"), metric_result.get("datasets_with_catalog"),
                    )
            except Exception:  # noqa: BLE001 — warmup nunca deve derrubar o processo
                _logger.exception("Falha ao sincronizar Gold Metric Catalog de %s", project_id)
        await asyncio.sleep(interval_hours * 3600)


__all__ = [
    "reindex_catalog",
    "search_catalog",
    "sync_gold_metric_catalog",
    "warmup_catalog_loop",
]
