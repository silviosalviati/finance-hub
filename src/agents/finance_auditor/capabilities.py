"""Catálogo de capabilities (tools) do Supervisor do Finance Voice IA.

Cada capability é uma função pura com assinatura
    fn(args: dict, context: dict) -> dict

`context` carrega: request_text, project_id, dataset_hint, llm, llm_creative,
tool_results (resultados de steps anteriores, para encadeamento).

O retorno é serializável (JSON-safe) com chaves:
    {"ok": bool, "payload": <dict|list|str>, "error": <str|None>, "artifacts": [...]}

Os wrappers BigQuery são finos — toda a lógica de cliente vive em
src/shared/tools/bigquery.py. Nenhuma capability tem domínio fixo (VoC,
fricção, sentimento, etc.): o Planner decide o que pedir a partir da pergunta
do usuário e do schema descoberto dinamicamente.
"""

from __future__ import annotations

import json
import re
import statistics
from typing import Any, Callable

from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.finance_auditor.supervisor_schemas import (
    CAPABILITY_BQ_GET_SCHEMA,
    CAPABILITY_BQ_LIST_DATASETS,
    CAPABILITY_BQ_LIST_TABLES,
    CAPABILITY_BQ_QUERY,
    CAPABILITY_CHAT_ANSWER,
    CAPABILITY_STATS_DESCRIBE,
    CAPABILITY_TEXT_TO_SQL,
    CAPABILITY_VIZ_SPEC,
)
from src.shared.config import get_runtime_config
from src.shared.tools.bigquery import (
    dry_run_query,
    execute_query_rows,
    get_dataset_tables_metadata,
    get_table_schema,
)
from src.shared.tools.llm import invoke_with_retry

# Budget máximo (bytes) por bq_query / text_to_sql. Default 5 GiB.
_DEFAULT_BQ_QUERY_BUDGET_BYTES = 5 * 1024 ** 3
_DEFAULT_BQ_QUERY_MAX_ROWS = 200
_SQL_FORBIDDEN_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|TRUNCATE|CREATE|ALTER|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
_SQL_FENCE_PATTERN = re.compile(r"```(?:sql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_TABLE_REF_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+$")


def _ok(payload: Any, artifacts: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"ok": True, "payload": payload, "error": None, "artifacts": artifacts or []}


def _err(message: str) -> dict[str, Any]:
    return {"ok": False, "payload": None, "error": message, "artifacts": []}


def _get_budget_bytes() -> int:
    return int(
        get_runtime_config(
            "FINANCE_AUDITOR_QUERY_BUDGET_BYTES", str(_DEFAULT_BQ_QUERY_BUDGET_BYTES)
        )
    )


def _llm_text(response: Any) -> str:
    return str(getattr(response, "content", response) or "")


def _resolve_project_for_table(table_ref: str, context_project: str | None) -> str:
    """Deriva o projeto a partir do table_ref (formato projeto.dataset.tabela).

    Cai para `context_project` apenas se o table_ref não estiver totalmente
    qualificado.
    """
    parts = (table_ref or "").split(".")
    if len(parts) == 3 and parts[0]:
        return parts[0]
    return (context_project or "").strip()


# ---------------------------------------------------------------------------
# bq_list_datasets
# ---------------------------------------------------------------------------

def cap_bq_list_datasets(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    project_id = (args.get("project_id") or context.get("project_id") or "").strip()
    if not project_id:
        return _err("project_id ausente.")
    try:
        # import local para manter o módulo importável em ambientes sem google-cloud
        from src.shared.tools.bigquery import _get_client  # noqa: WPS437

        client = _get_client(project_id)
        datasets = [ds.dataset_id for ds in client.list_datasets(project_id)]
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao listar datasets: {exc}")

    return _ok(
        payload={"project_id": project_id, "datasets": datasets},
        artifacts=[
            {
                "type": "table",
                "title": f"Datasets em {project_id}",
                "columns": ["dataset_id"],
                "rows": [{"dataset_id": d} for d in datasets],
            }
        ],
    )


# ---------------------------------------------------------------------------
# bq_list_tables
# ---------------------------------------------------------------------------

def cap_bq_list_tables(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    project_id = (context.get("project_id") or "").strip()
    dataset_hint = (
        args.get("dataset_hint")
        or context.get("dataset_hint")
        or get_runtime_config("FINANCE_AUDITOR_DEFAULT_DATASET", "")
    )
    if not project_id:
        return _err("project_id ausente para listar tabelas.")
    if not dataset_hint:
        return _err("dataset_hint ausente e sem default configurado.")
    try:
        info = get_dataset_tables_metadata(project_id, dataset_hint, max_tables=50, max_columns=20)
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao listar tabelas: {exc}")

    tables = info.get("tables", [])
    return _ok(
        payload={"dataset_ref": info.get("dataset_ref", ""), "tables": tables},
        artifacts=[
            {
                "type": "table",
                "title": f"Tabelas em {info.get('dataset_ref', '')}",
                "columns": ["table_id", "columns"],
                "rows": [
                    {"table_id": t.get("table_id", ""), "columns": ", ".join(t.get("columns", []) or [])}
                    for t in tables
                ],
            }
        ],
    )


# ---------------------------------------------------------------------------
# bq_get_schema
# ---------------------------------------------------------------------------

def cap_bq_get_schema(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    table_ref = str(args.get("table_ref") or "").strip()
    if not _TABLE_REF_PATTERN.match(table_ref):
        return _err("table_ref inválido. Use 'projeto.dataset.tabela'.")

    # O projeto vem do próprio table_ref para evitar consultar/billar o projeto
    # errado quando a tabela está em outro projeto que o usuário tem acesso.
    project_id = _resolve_project_for_table(table_ref, context.get("project_id"))
    if not project_id:
        return _err("Não foi possível determinar o projeto da tabela.")

    try:
        schema_text = get_table_schema(table_ref, project_id, max_columns=50)
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao obter schema: {exc}")

    return _ok(
        payload={"table_ref": table_ref, "project_id": project_id, "schema": schema_text},
        artifacts=[{"type": "schema", "table_ref": table_ref, "text": schema_text}],
    )


# ---------------------------------------------------------------------------
# bq_query — guardrails + dry-run + budget
# ---------------------------------------------------------------------------

def _validate_and_run_sql(
    sql: str,
    project_id: str,
    max_rows: int,
) -> dict[str, Any]:
    sql = (sql or "").strip().rstrip(";")
    if not sql:
        return _err("sql vazio.")
    if _SQL_FORBIDDEN_PATTERN.search(sql):
        return _err("Apenas queries de leitura (SELECT/WITH) são permitidas.")
    if not re.match(r"^\s*(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
        return _err("Apenas queries iniciando com SELECT ou WITH são permitidas.")
    if not project_id:
        return _err("project_id ausente para executar query.")

    max_rows = max(1, min(int(max_rows or _DEFAULT_BQ_QUERY_MAX_ROWS), 1000))
    budget = _get_budget_bytes()

    dry = dry_run_query(sql, project_id, timeout_seconds=20)
    if dry.error:
        return _err(f"Dry-run falhou: {dry.error}")
    if dry.bytes_processed > budget:
        gb = dry.bytes_processed / (1024 ** 3)
        gb_budget = budget / (1024 ** 3)
        return _err(
            f"Query excede o budget de {gb_budget:.2f} GiB "
            f"(estimado {gb:.2f} GiB). Refine os filtros."
        )

    try:
        rows = execute_query_rows(sql, project_id, max_rows=max_rows)
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha na execução: {exc}")

    columns = list(rows[0].keys()) if rows else []
    return _ok(
        payload={
            "sql": sql,
            "row_count": len(rows),
            "bytes_processed": dry.bytes_processed,
            "estimated_cost_usd": dry.estimated_cost_usd,
            "columns": columns,
            "rows": rows,
        },
        artifacts=[
            {"type": "sql", "sql": sql},
            {"type": "table", "title": "Resultado da query", "columns": columns, "rows": rows},
        ],
    )


def cap_bq_query(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    return _validate_and_run_sql(
        sql=str(args.get("sql") or ""),
        project_id=(context.get("project_id") or "").strip(),
        max_rows=int(args.get("max_rows") or _DEFAULT_BQ_QUERY_MAX_ROWS),
    )


# ---------------------------------------------------------------------------
# text_to_sql — LLM gera SQL a partir de NL + schemas
# ---------------------------------------------------------------------------

_TEXT_TO_SQL_PROMPT = """\
Você é um gerador de SQL para BigQuery. Receberá:
1. Uma pergunta em linguagem natural.
2. Os schemas das tabelas relevantes.

Devolva SOMENTE um JSON com o campo "sql" contendo a query SELECT/WITH \
(sem ponto-e-vírgula final, sem markdown, sem comentários longos). A query \
deve:
- Usar APENAS as tabelas listadas, sempre com nome totalmente qualificado \
(`projeto.dataset.tabela`).
- Aplicar LIMIT compatível com o pedido (default 200) salvo se o usuário \
pedir explicitamente uma agregação.
- Evitar SELECT *: liste colunas explicitamente quando possível.
- Não usar DDL/DML (INSERT/UPDATE/DELETE/CREATE/etc).

FORMATO:
{"sql": "SELECT ..."}
"""


def _extract_sql_from_llm_text(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    # tenta JSON puro
    try:
        data = json.loads(text)
        if isinstance(data, dict) and isinstance(data.get("sql"), str):
            return data["sql"].strip()
    except Exception:  # noqa: BLE001
        pass
    # tenta fenced block
    match = _SQL_FENCE_PATTERN.search(text)
    if match:
        return match.group(1).strip()
    # último recurso: o próprio texto
    return text


def cap_text_to_sql(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    natural_language = str(args.get("natural_language") or context.get("request_text") or "").strip()
    table_refs = [str(t).strip() for t in (args.get("table_refs") or []) if str(t).strip()]
    if not natural_language:
        return _err("natural_language ausente.")
    if not table_refs:
        return _err("table_refs ausente — informe ao menos uma tabela totalmente qualificada.")
    for ref in table_refs:
        if not _TABLE_REF_PATTERN.match(ref):
            return _err(f"table_ref inválido: {ref}")

    llm = context.get("llm")
    if llm is None:
        return _err("LLM indisponível neste contexto.")

    project_id = (context.get("project_id") or _resolve_project_for_table(table_refs[0], None)).strip()
    if not project_id:
        return _err("project_id ausente para text_to_sql.")

    # 1) Coleta schemas das tabelas
    schemas: list[str] = []
    for ref in table_refs[:5]:
        try:
            schemas.append(get_table_schema(ref, _resolve_project_for_table(ref, project_id), max_columns=50))
        except Exception as exc:  # noqa: BLE001
            schemas.append(f"[Falha ao obter schema de {ref}: {exc}]")
    schemas_text = "\n\n".join(schemas) or "(sem schemas)"

    # 2) Gera SQL via LLM
    user_msg = (
        f"PERGUNTA:\n{natural_language}\n\n"
        f"SCHEMAS DISPONÍVEIS:\n{schemas_text}\n\n"
        "Devolva o JSON pedido."
    )
    try:
        response = invoke_with_retry(
            llm,
            [SystemMessage(content=_TEXT_TO_SQL_PROMPT), HumanMessage(content=user_msg)],
            max_attempts=2,
        )
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao gerar SQL via LLM: {exc}")

    sql = _extract_sql_from_llm_text(_llm_text(response))
    if not sql:
        return _err("LLM não devolveu SQL utilizável.")

    # 3) Valida e executa (reutiliza o mesmo pipeline de bq_query)
    row_limit = int(args.get("row_limit") or _DEFAULT_BQ_QUERY_MAX_ROWS)
    exec_result = _validate_and_run_sql(sql=sql, project_id=project_id, max_rows=row_limit)
    if exec_result["ok"]:
        # Inclui também a pergunta original no payload para rastreabilidade.
        exec_result["payload"]["natural_language"] = natural_language
        exec_result["payload"]["table_refs"] = table_refs
    else:
        # Mesmo em falha, devolve o SQL para o usuário poder inspecionar.
        exec_result["payload"] = {"attempted_sql": sql, "natural_language": natural_language}
        exec_result["artifacts"] = [{"type": "sql", "sql": sql}]
    return exec_result


# ---------------------------------------------------------------------------
# stats_describe — estatística descritiva sobre rows de step anterior
# ---------------------------------------------------------------------------

def _is_number(v: Any) -> bool:
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return True
    if isinstance(v, str):
        try:
            float(v)
            return True
        except ValueError:
            return False
    return False


def _to_float(v: Any) -> float:
    return float(v) if not isinstance(v, str) else float(v)


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    k = (len(sorted_values) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = k - lo
    return float(sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac)


def _describe_numeric(values: list[float]) -> dict[str, Any]:
    sv = sorted(values)
    n = len(sv)
    return {
        "count": n,
        "mean": round(statistics.fmean(sv), 6) if n else None,
        "median": round(statistics.median(sv), 6) if n else None,
        "stdev": round(statistics.pstdev(sv), 6) if n > 1 else 0.0,
        "min": float(sv[0]) if n else None,
        "p25": round(_percentile(sv, 0.25), 6) if n else None,
        "p75": round(_percentile(sv, 0.75), 6) if n else None,
        "max": float(sv[-1]) if n else None,
    }


def _describe_categorical(values: list[Any]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for v in values:
        key = "" if v is None else str(v)
        counts[key] = counts.get(key, 0) + 1
    top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
    return {
        "count": len(values),
        "distinct": len(counts),
        "top": [{"value": k, "count": v} for k, v in top],
    }


def _resolve_source_rows(
    args: dict[str, Any], context: dict[str, Any]
) -> tuple[list[dict[str, Any]], str | None]:
    rows = args.get("rows")
    if isinstance(rows, list):
        return rows, None
    idx = args.get("source_step_index")
    if idx is None:
        return [], "source_step_index ausente."
    try:
        idx = int(idx)
    except (TypeError, ValueError):
        return [], "source_step_index inválido."
    prior = context.get("tool_results") or []
    if not (0 <= idx < len(prior)):
        return [], f"source_step_index fora do intervalo (0..{len(prior) - 1})."
    payload = (prior[idx] or {}).get("payload") or {}
    if not (prior[idx] or {}).get("ok"):
        return [], f"Step {idx} não foi bem-sucedido — nada para analisar."
    src_rows = payload.get("rows")
    if not isinstance(src_rows, list):
        return [], f"Step {idx} não produziu uma lista de rows."
    return src_rows, None


def cap_stats_describe(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    rows, err = _resolve_source_rows(args, context)
    if err:
        return _err(err)
    if not rows:
        return _ok(payload={"summary": "Sem linhas para analisar.", "columns": {}})

    selected = args.get("columns") or []
    if selected:
        keys = [str(c) for c in selected]
    else:
        keys = list(rows[0].keys())

    stats: dict[str, Any] = {}
    for col in keys:
        raw = [r.get(col) for r in rows]
        non_null = [v for v in raw if v is not None]
        nums = [v for v in non_null if _is_number(v)]
        if nums and len(nums) >= max(1, int(0.6 * len(non_null) or 1)):
            stats[col] = {"type": "numeric", **_describe_numeric([_to_float(v) for v in nums])}
        else:
            stats[col] = {"type": "categorical", **_describe_categorical(non_null)}

    return _ok(
        payload={"row_count": len(rows), "columns": stats},
        artifacts=[
            {
                "type": "stats",
                "row_count": len(rows),
                "columns": stats,
            }
        ],
    )


# ---------------------------------------------------------------------------
# viz_spec — Vega-Lite JSON para frontend renderizar
# ---------------------------------------------------------------------------

_VALID_CHART_TYPES = {"bar", "line", "area", "point", "arc"}


def _infer_field_type(values: list[Any]) -> str:
    sample = [v for v in values if v is not None][:50]
    if not sample:
        return "nominal"
    if all(_is_number(v) for v in sample):
        return "quantitative"
    if all(isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}", v) for v in sample):
        return "temporal"
    return "nominal"


def cap_viz_spec(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    rows, err = _resolve_source_rows(args, context)
    if err:
        return _err(err)
    if not rows:
        return _err("Sem linhas para gerar gráfico.")

    chart_type = str(args.get("chart_type") or "bar").lower()
    if chart_type not in _VALID_CHART_TYPES:
        return _err(f"chart_type inválido: {chart_type}. Use um de {sorted(_VALID_CHART_TYPES)}.")

    x = str(args.get("x") or "").strip()
    y = str(args.get("y") or "").strip()
    if not x or not y:
        return _err("x e y são obrigatórios.")
    if x not in rows[0] or y not in rows[0]:
        return _err("x ou y não existem nas colunas do step de origem.")

    color = str(args.get("color") or "").strip() or None
    title = str(args.get("title") or "").strip()

    x_type = _infer_field_type([r.get(x) for r in rows])
    y_type = _infer_field_type([r.get(y) for r in rows])

    encoding: dict[str, Any] = {
        "x": {"field": x, "type": x_type},
        "y": {"field": y, "type": y_type},
    }
    if color and color in rows[0]:
        encoding["color"] = {"field": color, "type": _infer_field_type([r.get(color) for r in rows])}

    spec: dict[str, Any] = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": rows[:500]},  # cap defensivo para payload do frontend
        "mark": {"type": chart_type, "tooltip": True},
        "encoding": encoding,
    }
    if title:
        spec["title"] = title

    return _ok(
        payload={"chart_type": chart_type, "row_count": len(rows), "title": title},
        artifacts=[{"type": "vega_lite", "title": title or f"{chart_type} chart", "spec": spec}],
    )


# ---------------------------------------------------------------------------
# chat_answer
# ---------------------------------------------------------------------------

def cap_chat_answer(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    return _ok(payload={"note": "Resposta conversacional sem consulta a dados."})


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

CAPABILITY_REGISTRY: dict[str, Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]] = {
    CAPABILITY_BQ_LIST_DATASETS: cap_bq_list_datasets,
    CAPABILITY_BQ_LIST_TABLES: cap_bq_list_tables,
    CAPABILITY_BQ_GET_SCHEMA: cap_bq_get_schema,
    CAPABILITY_BQ_QUERY: cap_bq_query,
    CAPABILITY_TEXT_TO_SQL: cap_text_to_sql,
    CAPABILITY_STATS_DESCRIBE: cap_stats_describe,
    CAPABILITY_VIZ_SPEC: cap_viz_spec,
    CAPABILITY_CHAT_ANSWER: cap_chat_answer,
}


def execute_capability(
    capability: str, args: dict[str, Any], context: dict[str, Any]
) -> dict[str, Any]:
    fn = CAPABILITY_REGISTRY.get(capability)
    if fn is None:
        return _err(f"Capability desconhecida: {capability}")
    return fn(args or {}, context or {})


__all__ = [
    "CAPABILITY_REGISTRY",
    "execute_capability",
    "cap_bq_list_datasets",
    "cap_bq_list_tables",
    "cap_bq_get_schema",
    "cap_bq_query",
    "cap_text_to_sql",
    "cap_stats_describe",
    "cap_viz_spec",
    "cap_chat_answer",
]
