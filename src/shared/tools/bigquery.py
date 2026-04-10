from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from google.cloud import bigquery
from google.cloud import datacatalog_v1
from google.cloud.exceptions import GoogleCloudError
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account

from src.shared.config import (
    BQ_COST_PER_TB_USD,
    BYTES_WARNING_THRESHOLD,
    GCP_CREDENTIALS_PATH,
    GCP_PROJECT_ID,
)
from src.shared.tools.schemas import DryRunResult
from src.shared.utils.formatting import format_bytes

TABLE_PATTERN = r"`?([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)`?"


def _resolve_project_id(project_id: str | None) -> str:
    resolved = (project_id or GCP_PROJECT_ID).strip()
    if not resolved:
        raise ValueError("Project ID do BigQuery nao informado.")
    return resolved


@lru_cache(maxsize=1)
def _get_base_credentials():
    return service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)


def _get_client(project_id: str | None) -> bigquery.Client:
    resolved_project_id = _resolve_project_id(project_id)
    credentials = _get_base_credentials()

    return bigquery.Client(
        project=resolved_project_id,
        credentials=credentials,
    )


def _parse_dataset_hint(project_id: str, dataset_hint: str) -> tuple[str, str]:
    cleaned = dataset_hint.strip().strip("`")
    if not cleaned:
        raise ValueError("Dataset hint nao pode ser vazio.")

    parts = cleaned.split(".")
    if len(parts) == 1:
        return project_id, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]

    raise ValueError(
        "Dataset hint invalido. Use apenas 'dataset' ou 'project.dataset'."
    )


def validate_dataset_for_query_build(project_id: str, dataset_hint: str) -> dict[str, Any]:
    resolved_project = _resolve_project_id(project_id)
    dataset_project, dataset_id = _parse_dataset_hint(resolved_project, dataset_hint)
    dataset_ref = f"{dataset_project}.{dataset_id}"

    bigquery_ok = False
    datacatalog_ok = False
    table_count = 0
    datacatalog_error: str | None = None

    try:
        client = _get_client(dataset_project)
        client.get_dataset(dataset_ref)
        bigquery_ok = True
        table_count = sum(1 for _ in client.list_tables(dataset_ref))
    except NotFound:
        return {
            "valid": False,
            "project_id": dataset_project,
            "dataset_id": dataset_id,
            "dataset_ref": dataset_ref,
            "bigquery_ok": False,
            "datacatalog_ok": False,
            "table_count": 0,
            "message": "Dataset nao encontrado no BigQuery.",
        }
    except GoogleCloudError as exc:
        return {
            "valid": False,
            "project_id": dataset_project,
            "dataset_id": dataset_id,
            "dataset_ref": dataset_ref,
            "bigquery_ok": False,
            "datacatalog_ok": False,
            "table_count": 0,
            "message": f"Falha ao validar dataset no BigQuery: {exc}",
        }

    linked_resource = (
        f"//bigquery.googleapis.com/projects/{dataset_project}/datasets/{dataset_id}"
    )

    try:
        catalog_client = datacatalog_v1.DataCatalogClient(credentials=_get_base_credentials())
        entry = catalog_client.lookup_entry(
            request={"linked_resource": linked_resource}
        )
        datacatalog_ok = bool(getattr(entry, "name", ""))
    except NotFound:
        datacatalog_ok = False
    except Exception as exc:
        datacatalog_ok = False
        datacatalog_error = str(exc)

    valid = bigquery_ok and datacatalog_ok
    if valid:
        message = (
            "Perfeito! Dataset validado com sucesso. "
            f"Encontramos {table_count} tabela(s) com metadados prontos para uso."
        )
    elif datacatalog_error:
        message = (
            "Dataset existe no BigQuery, mas nao foi possivel validar metadados no Data Catalog: "
            f"{datacatalog_error}"
        )
    else:
        message = (
            "Dataset existe no BigQuery, mas nao esta indexado/visivel no Data Catalog."
        )

    return {
        "valid": valid,
        "project_id": dataset_project,
        "dataset_id": dataset_id,
        "dataset_ref": dataset_ref,
        "bigquery_ok": bigquery_ok,
        "datacatalog_ok": datacatalog_ok,
        "table_count": table_count,
        "message": message,
    }


def validate_query_context_for_query_analyzer(
    query: str,
    project_id: str | None = None,
) -> dict[str, Any]:
    normalized_query = (query or "").strip()
    if not normalized_query:
        raise ValueError("Query nao pode ser vazia.")

    table_refs = sorted({ref.strip() for ref in re.findall(TABLE_PATTERN, normalized_query)})
    if not table_refs:
        return {
            "valid": False,
            "project_id": "",
            "dataset_id": "",
            "dataset_hint": "",
            "dataset_ref": "",
            "table_count": 0,
            "matched_tables": [],
            "missing_tables": [],
            "message": (
                "Nao foi possivel identificar tabelas no formato project.dataset.tabela."
            ),
        }

    split_refs = [table_ref.split(".") for table_ref in table_refs]
    datasets_found = {(parts[0], parts[1]) for parts in split_refs}
    if len(datasets_found) != 1:
        return {
            "valid": False,
            "project_id": "",
            "dataset_id": "",
            "dataset_hint": "",
            "dataset_ref": "",
            "table_count": 0,
            "matched_tables": [],
            "missing_tables": [],
            "message": "A query deve referenciar apenas um dataset por analise.",
        }

    query_project_id, dataset_id = next(iter(datasets_found))
    resolved_project_id = _resolve_project_id(project_id or query_project_id)

    if query_project_id != resolved_project_id:
        return {
            "valid": False,
            "project_id": resolved_project_id,
            "dataset_id": dataset_id,
            "dataset_hint": dataset_id,
            "dataset_ref": f"{resolved_project_id}.{dataset_id}",
            "table_count": 0,
            "matched_tables": [],
            "missing_tables": [],
            "message": "Project ID da query diferente do Project ID detectado.",
        }

    dataset_validation = validate_dataset_for_query_build(
        project_id=resolved_project_id,
        dataset_hint=dataset_id,
    )
    if not dataset_validation.get("valid"):
        return {
            **dataset_validation,
            "dataset_hint": dataset_id,
            "matched_tables": [],
            "missing_tables": [],
        }

    query_table_ids = sorted({parts[2].strip() for parts in split_refs})
    matched_tables: list[str] = []
    missing_tables: list[str] = []
    client = _get_client(resolved_project_id)

    for table_id in query_table_ids:
        table_ref = f"{resolved_project_id}.{dataset_id}.{table_id}"
        try:
            client.get_table(table_ref)
            matched_tables.append(table_id)
        except NotFound:
            missing_tables.append(table_id)
        except GoogleCloudError:
            missing_tables.append(table_id)

    if missing_tables:
        return {
            **dataset_validation,
            "valid": False,
            "dataset_hint": dataset_id,
            "matched_tables": matched_tables,
            "missing_tables": missing_tables,
            "message": (
                "Dataset validado, mas a query referencia tabela(s) inexistente(s): "
                + ", ".join(missing_tables)
            ),
        }

    return {
        **dataset_validation,
        "valid": True,
        "dataset_hint": dataset_id,
        "matched_tables": matched_tables,
        "missing_tables": [],
        "message": (
            "Perfeito! Dataset e tabelas da query validados no BigQuery e no Data Catalog/Dataplex."
        ),
    }


def get_dataset_tables_metadata(
    project_id: str,
    dataset_hint: str,
    max_tables: int = 20,
    max_columns: int = 15,
) -> dict[str, Any]:
    resolved_project = _resolve_project_id(project_id)
    dataset_project, dataset_id = _parse_dataset_hint(resolved_project, dataset_hint)
    dataset_ref = f"{dataset_project}.{dataset_id}"

    client = _get_client(dataset_project)
    client.get_dataset(dataset_ref)

    tables_info: list[dict[str, Any]] = []
    for table_item in client.list_tables(dataset_ref):
        table_ref = f"{dataset_project}.{dataset_id}.{table_item.table_id}"
        columns: list[str] = []
        try:
            table = client.get_table(table_ref)
            columns = [field.name for field in table.schema[:max_columns]]
        except Exception:
            columns = []

        tables_info.append(
            {
                "table_id": table_item.table_id,
                "full_name": table_ref,
                "columns": columns,
            }
        )

        if len(tables_info) >= max_tables:
            break

    return {
        "project_id": dataset_project,
        "dataset_id": dataset_id,
        "dataset_ref": dataset_ref,
        "tables": tables_info,
    }


def _build_error_result(message: str) -> DryRunResult:
    return DryRunResult(
        bytes_processed=0,
        bytes_billed=0,
        estimated_cost_usd=0.0,
        referenced_tables=[],
        error=message,
    )


def _extract_referenced_tables(job) -> list[str]:
    if not job.referenced_tables:
        return []

    return [
        f"{table.project}.{table.dataset_id}.{table.table_id}"
        for table in job.referenced_tables
    ]


def dry_run_query(query: str, project_id: str | None) -> DryRunResult:
    try:
        client = _get_client(project_id)
        job_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
        )

        job = client.query(query, job_config=job_config)

        bytes_processed = job.total_bytes_processed or 0
        bytes_billed = job.total_bytes_billed or bytes_processed
        tb_processed = bytes_billed / (1024**4)
        estimated_cost = round(tb_processed * BQ_COST_PER_TB_USD, 6)

        referenced_tables = _extract_referenced_tables(job)

        return DryRunResult(
            bytes_processed=bytes_processed,
            bytes_billed=bytes_billed,
            estimated_cost_usd=estimated_cost,
            referenced_tables=referenced_tables,
        )

    except GoogleCloudError as exc:
        return _build_error_result(str(exc))
    except Exception as exc:
        return _build_error_result(f"Erro inesperado no dry-run: {exc}")


def fetch_query_sample(
    query: str,
    project_id: str | None,
    limit: int = 10,
) -> dict[str, Any]:
    if not query or not query.strip():
        return {
            "columns": [],
            "rows": [],
            "error": "Query vazia para amostra de dados.",
        }

    safe_limit = max(1, min(limit, 10))
    normalized_query = _normalize_query_for_subquery(query)
    sample_query = f"SELECT * FROM ({normalized_query}) LIMIT {safe_limit}"

    try:
        client = _get_client(project_id)
        job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            maximum_bytes_billed=BYTES_WARNING_THRESHOLD,
        )
        job = client.query(sample_query, job_config=job_config)
        result = job.result(max_results=safe_limit)

        rows = [dict(row.items()) for row in result]
        columns = list(rows[0].keys()) if rows else []

        return {
            "columns": columns,
            "rows": rows,
            "error": None,
        }
    except GoogleCloudError as exc:
        return {
            "columns": [],
            "rows": [],
            "error": f"Falha ao buscar amostra de dados: {exc}",
        }
    except Exception as exc:
        return {
            "columns": [],
            "rows": [],
            "error": f"Erro inesperado na amostra de dados: {exc}",
        }


def _normalize_query_for_subquery(query: str) -> str:
    cleaned = query.strip()
    while cleaned.endswith(";"):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


def get_table_schema(table_ref: str, project_id: str | None) -> str:
    try:
        client = _get_client(project_id)
        table = client.get_table(table_ref)

        lines = [f"Tabela: {table_ref}"]

        if table.time_partitioning:
            partition_field = table.time_partitioning.field or "ingestion time"
            lines.append(f"  Particionada por: {partition_field}")

        if table.clustering_fields:
            lines.append(f"  Clusterizada por: {', '.join(table.clustering_fields)}")

        lines.append("  Colunas:")
        for field in table.schema:
            description = f" - {field.description}" if field.description else ""
            lines.append(f"    - {field.name} ({field.field_type}, {field.mode}){description}")

        return "\n".join(lines)

    except Exception as exc:
        return f"[Nao foi possivel obter schema de {table_ref}: {exc}]"


def get_schemas_for_query(query: str, project_id: str | None) -> str:
    tables = list(set(re.findall(TABLE_PATTERN, query)))
    if not tables:
        return "(schema nao disponivel - nenhuma tabela totalmente qualificada encontrada)"

    schemas = [get_table_schema(table_ref, project_id) for table_ref in tables[:5]]
    return "\n\n".join(schemas)


__all__ = [
    "dry_run_query",
    "get_table_schema",
    "get_schemas_for_query",
    "fetch_query_sample",
    "validate_dataset_for_query_build",
    "validate_query_context_for_query_analyzer",
    "get_dataset_tables_metadata",
    "format_bytes",
]
