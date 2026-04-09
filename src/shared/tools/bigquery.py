from __future__ import annotations

import re
from functools import lru_cache

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.oauth2 import service_account

from src.shared.config import BQ_COST_PER_TB_USD, GCP_CREDENTIALS_PATH, GCP_PROJECT_ID
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
    "format_bytes",
]
