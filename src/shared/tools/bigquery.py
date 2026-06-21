from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from google.cloud import bigquery
from google.cloud import dataplex_v1
from google.cloud.exceptions import GoogleCloudError
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account

from src.shared.config import get_runtime_config
from src.shared.tools.schemas import DryRunResult
from src.shared.utils.formatting import format_bytes

TABLE_PATTERN = r"`?([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)`?"

_DEFAULT_CREDENTIALS_PATH = str(Path("secrets") / "credentials.json")
_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _resolve_project_id(project_id: str | None) -> str:
    from src.shared.config import get_default_gcp_project
    resolved = (project_id or get_default_gcp_project()).strip()
    if not resolved:
        raise ValueError("Project ID do BigQuery nao informado.")
    return resolved


@lru_cache(maxsize=4)
def _get_base_credentials(credentials_path: str):
    return service_account.Credentials.from_service_account_file(credentials_path)


def _resolve_credentials_path(credentials_path: str | None) -> str:
    configured = (credentials_path or "").strip() or _DEFAULT_CREDENTIALS_PATH
    candidate = Path(configured).expanduser()

    if candidate.is_absolute():
        return str(candidate)

    # Resolve relativo a raiz do projeto para evitar depender do CWD.
    return str((_PROJECT_ROOT / candidate).resolve())


def _get_credentials_path() -> str:
    configured = get_runtime_config(
        "GOOGLE_APPLICATION_CREDENTIALS", _DEFAULT_CREDENTIALS_PATH
    )
    return _resolve_credentials_path(configured)


@lru_cache(maxsize=16)
def _get_client(project_id: str | None) -> bigquery.Client:
    # bigquery.Client é seguro para uso concorrente (mesmo padrão de reuso
    # recomendado pela própria lib) — recriar um por chamada só pagava
    # overhead de conexão/transporte sem ganhar nada em troca. Limite de 16
    # entradas evita crescimento ilimitado caso project_id venha de input
    # do usuário com valores variados.
    resolved_project_id = _resolve_project_id(project_id)
    credentials_path = _get_credentials_path()
    credentials = _get_base_credentials(credentials_path)

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


def validate_dataset_for_query_build(
    project_id: str,
    dataset_hint: str,
    require_datacatalog: bool = True,
) -> dict[str, Any]:
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

    if require_datacatalog:
        try:
            dataplex_client = dataplex_v1.CatalogServiceClient(
                credentials=_get_base_credentials(_get_credentials_path())
            )
            request = dataplex_v1.SearchEntriesRequest(
                name=f"projects/{dataset_project}/locations/global",
                query=f"{dataset_project}.{dataset_id} system=BIGQUERY",
                page_size=10,
            )

            results = list(dataplex_client.search_entries(request=request))
            dataset_marker = f"/datasets/{dataset_id}".lower()
            datacatalog_ok = any(
                dataset_marker in str(getattr(result, "linked_resource", "")).lower()
                for result in results
            )
        except NotFound:
            datacatalog_ok = False
        except Exception as exc:
            datacatalog_ok = False
            datacatalog_error = str(exc)

    valid = bigquery_ok and (datacatalog_ok if require_datacatalog else True)
    if valid:
        if require_datacatalog:
            message = (
                "Perfeito! Dataset validado com sucesso. "
                f"Encontramos {table_count} tabela(s) com metadados prontos para uso."
            )
        else:
            message = (
                "Perfeito! Dataset validado no BigQuery com sucesso. "
                f"Encontramos {table_count} tabela(s) disponiveis para analise."
            )
    elif require_datacatalog and datacatalog_error:
        message = (
            "Dataset existe no BigQuery, mas nao foi possivel validar metadados no Data Catalog: "
            f"{datacatalog_error}"
        )
    elif require_datacatalog:
        message = (
            "Dataset existe no BigQuery, mas nao esta indexado/visivel no Data Catalog."
        )
    else:
        message = "Dataset nao pode ser validado no BigQuery."

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
        require_datacatalog=False,
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
    dataset_ref = f"{resolved_project_id}.{dataset_id}"
    client = _get_client(resolved_project_id)

    try:
        existing_table_ids = {table.table_id for table in client.list_tables(dataset_ref)}
    except GoogleCloudError:
        existing_table_ids = set()

    for table_id in query_table_ids:
        if table_id in existing_table_ids:
            matched_tables.append(table_id)
        else:
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
        "datacatalog_ok": dataset_validation.get("datacatalog_ok", False),
        "dataset_hint": dataset_id,
        "matched_tables": matched_tables,
        "missing_tables": [],
        "message": (
            "Perfeito! Dataset e tabelas da query validados no BigQuery e no Data Catalog/Dataplex."
        ),
    }


def list_table_ids(project_id: str, dataset_id: str) -> list[str]:
    """Lista os IDs de tabelas de um dataset — checagem leve de existência.

    Usado para descobrir, sem hardcoded de nome de dataset, em qual(is)
    dataset(s) de um projeto existe uma tabela de convenção fixa (ex.:
    `GOLD_METRIC_CATALOG`) — cada gerência tem o seu próprio dataset, então a
    varredura precisa ser por todos os datasets do projeto, não um fixo.
    """
    resolved_project = _resolve_project_id(project_id)
    client = _get_client(resolved_project)
    dataset_ref = f"{resolved_project}.{dataset_id}"
    return [t.table_id for t in client.list_tables(dataset_ref)]


def list_datasets_with_labels(project_id: str) -> list[dict[str, Any]]:
    """Lista datasets do projeto com seus rotulos (labels) do BigQuery.

    `DatasetListItem.labels` ja vem populado pela API de listagem — nao ha
    custo extra de uma chamada `get_dataset` por dataset.
    """
    resolved_project = _resolve_project_id(project_id)
    client = _get_client(resolved_project)
    return [
        {"dataset_id": ds.dataset_id, "labels": dict(ds.labels or {})}
        for ds in client.list_datasets(resolved_project)
    ]


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


def get_dataset_tables_schema(
    project_id: str,
    dataset_hint: str,
    max_tables: int = 20,
    max_columns: int = 50,
) -> dict[str, Any]:
    resolved_project = _resolve_project_id(project_id)
    dataset_project, dataset_id = _parse_dataset_hint(resolved_project, dataset_hint)
    dataset_ref = f"{dataset_project}.{dataset_id}"

    client = _get_client(dataset_project)
    client.get_dataset(dataset_ref)

    tables_info: list[dict[str, Any]] = []
    for table_item in client.list_tables(dataset_ref):
        table_ref = f"{dataset_project}.{dataset_id}.{table_item.table_id}"
        columns: list[dict[str, str]] = []
        partition_field = ""
        clustering_fields: list[str] = []

        try:
            table = client.get_table(table_ref)
            partition_field = (table.time_partitioning.field if table.time_partitioning else "") or ""
            clustering_fields = list(table.clustering_fields or [])

            for field in table.schema[:max_columns]:
                columns.append(
                    {
                        "name": field.name,
                        "type": field.field_type,
                        "mode": field.mode,
                        "description": field.description or "",
                    }
                )
        except Exception:
            columns = []
            partition_field = ""
            clustering_fields = []

        tables_info.append(
            {
                "table_id": table_item.table_id,
                "full_name": table_ref,
                "partition_field": partition_field,
                "clustering_fields": clustering_fields,
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


def dry_run_query(
    query: str,
    project_id: str | None,
    timeout_seconds: float | None = None,
) -> DryRunResult:
    try:
        client = _get_client(project_id)
        job_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
        )

        job = client.query(query, job_config=job_config)
        # Garantimos que os metadados do dry-run estejam populados antes do calculo.
        if timeout_seconds is not None:
            job.result(timeout=timeout_seconds)
        else:
            job.result()

        bytes_processed = job.total_bytes_processed or 0
        bytes_billed = job.total_bytes_billed or bytes_processed
        tb_processed = bytes_billed / (1024**4)
        bq_cost_per_tb = float(get_runtime_config("BQ_COST_PER_TB_USD", "5.0"))
        estimated_cost = round(tb_processed * bq_cost_per_tb, 6)

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
        bytes_warning = int(get_runtime_config("BYTES_WARNING_THRESHOLD", str(10 * 1024**3)))
        job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            maximum_bytes_billed=bytes_warning,
        )
        job = client.query(sample_query, job_config=job_config)
        result = job.result(max_results=safe_limit)

        rows = [_json_safe_row(dict(row.items())) for row in result]
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


def get_table_column_types(table_ref: str, project_id: str | None) -> dict[str, str]:
    """Mapa {nome_coluna: tipo_bigquery} de uma tabela — usado para escolher
    dinamicamente qual coluna é a data de referência de uma métrica do Gold
    Metric Catalog, sem precisar de nenhuma convenção hardcoded por gerência.
    """
    try:
        client = _get_client(project_id)
        table = client.get_table(table_ref)
        return {field.name: field.field_type for field in table.schema}
    except Exception:
        return {}


def get_table_schema(
    table_ref: str,
    project_id: str | None,
    max_columns: int = 50,
) -> str:
    try:
        client = _get_client(project_id)
        table = client.get_table(table_ref)

        lines = [f"Tabela: {table_ref}"]

        if table.time_partitioning:
            partition_field = table.time_partitioning.field or "ingestion time"
            lines.append(f"  Particionada por: {partition_field}")

        if table.clustering_fields:
            lines.append(f"  Clusterizada por: {', '.join(table.clustering_fields)}")

        total_cols = len(table.schema)
        lines.append("  Colunas:")
        for field in table.schema[:max_columns]:
            description = f" — {field.description}" if field.description else ""
            lines.append(f"    - {field.name} ({field.field_type}, {field.mode}){description}")

        if total_cols > max_columns:
            lines.append(f"    ... (+{total_cols - max_columns} colunas omitidas)")

        return "\n".join(lines)

    except Exception as exc:
        return f"[Nao foi possivel obter schema de {table_ref}: {exc}]"


def get_schemas_for_query(query: str, project_id: str | None, max_columns: int = 50) -> str:
    tables = list(set(re.findall(TABLE_PATTERN, query)))
    if not tables:
        return "(schema nao disponivel - nenhuma tabela totalmente qualificada encontrada)"

    schemas = [get_table_schema(t, project_id, max_columns=max_columns) for t in tables[:5]]
    return "\n\n".join(schemas)


def execute_query_rows(
    query: str,
    project_id: str | None,
    max_rows: int = 1000,
) -> list[dict[str, Any]]:
    """Executa uma query BigQuery e retorna as linhas como lista de dicionários.

    Diferente de :func:`fetch_query_sample`, não envolve a query em um
    subselect, sendo adequado para queries de agregação e contagem.

    Args:
        query: SQL completo a ser executado.
        project_id: Projeto GCP de faturamento.
        max_rows: Limite de linhas retornadas (padrão: 1000).

    Returns:
        Lista de dicionários {coluna: valor} com até *max_rows* linhas.

    Raises:
        RuntimeError: Se a execução no BigQuery falhar.
    """
    try:
        client = _get_client(project_id)
        job_config = bigquery.QueryJobConfig(use_query_cache=False)
        job = client.query(query, job_config=job_config)
        result = job.result(max_results=max_rows)
        return [_json_safe_row(dict(row.items())) for row in result]
    except GoogleCloudError as exc:
        raise RuntimeError(f"Falha na execução da query BigQuery: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Erro inesperado ao executar query: {exc}") from exc


def _json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_safe_value(value) for key, value in row.items()}


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, Decimal):
        # Preserve precision for NUMERIC/BIGNUMERIC instead of forcing float.
        return str(value)

    if isinstance(value, (datetime, date, time)):
        return value.isoformat()

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]

    return value


__all__ = [
    "dry_run_query",
    "execute_query_rows",
    "get_table_schema",
    "get_table_column_types",
    "get_schemas_for_query",
    "fetch_query_sample",
    "validate_dataset_for_query_build",
    "validate_query_context_for_query_analyzer",
    "list_datasets_with_labels",
    "list_table_ids",
    "get_dataset_tables_metadata",
    "get_dataset_tables_schema",
    "format_bytes",
]
