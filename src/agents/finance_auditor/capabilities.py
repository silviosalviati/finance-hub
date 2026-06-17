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

import difflib
import json
import re
import statistics
import unicodedata
from typing import Any, Callable

from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.finance_auditor import forecast as forecast_mod
from src.agents.finance_auditor import multimodal, org_memory, rbac, semantic_layer
from src.agents.finance_auditor.supervisor_schemas import (
    CAPABILITY_ATTACHMENT_ANALYZE,
    CAPABILITY_BQ_GET_SCHEMA,
    CAPABILITY_BQ_LIST_DATASETS,
    CAPABILITY_BQ_LIST_TABLES,
    CAPABILITY_BQ_QUERY,
    CAPABILITY_CHAT_ANSWER,
    CAPABILITY_FORECAST_SIMPLE,
    CAPABILITY_METRIC_EXECUTE,
    CAPABILITY_METRIC_LOOKUP,
    CAPABILITY_ORG_FACT_RECALL,
    CAPABILITY_ORG_FACT_SAVE,
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

def _slug(text: str) -> str:
    """Normaliza string para fuzzy match (sem acentos, lowercase, só [a-z0-9_])."""
    s = unicodedata.normalize("NFD", text or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    return re.sub(r"[^a-z0-9_]+", "_", s).strip("_")


def _fuzzy_pick_dataset(hint: str, datasets: list[str]) -> str | None:
    """Escolhe o dataset com maior similaridade ao hint (>= 0.4) ou substring match."""
    if not hint or not datasets:
        return None
    hint_slug = _slug(hint)
    if not hint_slug:
        return None

    # 1) match exato após normalização
    for ds in datasets:
        if _slug(ds) == hint_slug:
            return ds
    # 2) substring (em qualquer direção)
    for ds in datasets:
        ds_slug = _slug(ds)
        if hint_slug in ds_slug or ds_slug in hint_slug:
            return ds
    # 3) similaridade via difflib
    scored = sorted(
        ((difflib.SequenceMatcher(None, hint_slug, _slug(ds)).ratio(), ds) for ds in datasets),
        reverse=True,
    )
    if scored and scored[0][0] >= 0.4:
        return scored[0][1]
    return None


def _list_project_datasets(project_id: str) -> list[str]:
    from src.shared.tools.bigquery import _get_client  # noqa: WPS437

    client = _get_client(project_id)
    return [ds.dataset_id for ds in client.list_datasets(project_id)]


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

    allowed, reason = rbac.check_dataset(context.get("user"), dataset_hint)
    if not allowed:
        return _err(f"RBAC: {reason}")

    info = None
    resolved_hint = dataset_hint
    fallback_note = ""
    try:
        info = get_dataset_tables_metadata(project_id, dataset_hint, max_tables=50, max_columns=20)
    except Exception as exc:  # noqa: BLE001
        # Autocorreção: se o dataset não existe, lista o projeto e tenta fuzzy match.
        if "not found" in str(exc).lower() or "notfound" in str(exc).lower() or "404" in str(exc):
            try:
                available = _list_project_datasets(project_id)
            except Exception as list_exc:  # noqa: BLE001
                return _err(
                    f"Dataset '{dataset_hint}' não existe e falhou ao listar projeto: {list_exc}"
                )
            pick = _fuzzy_pick_dataset(dataset_hint, available)
            if not pick:
                return _err(
                    f"Dataset '{dataset_hint}' não encontrado. "
                    f"Datasets disponíveis: {', '.join(available) or '(nenhum)'}"
                )
            allowed_pick, reason_pick = rbac.check_dataset(context.get("user"), pick)
            if not allowed_pick:
                return _err(
                    f"RBAC: dataset '{dataset_hint}' inexistente; "
                    f"substituto '{pick}' negado ({reason_pick})."
                )
            try:
                info = get_dataset_tables_metadata(project_id, pick, max_tables=50, max_columns=20)
                resolved_hint = pick
                fallback_note = (
                    f"Dataset '{dataset_hint}' não existe; usado '{pick}' por correspondência."
                )
            except Exception as retry_exc:  # noqa: BLE001
                return _err(f"Falha ao listar tabelas de '{pick}': {retry_exc}")
        else:
            return _err(f"Falha ao listar tabelas: {exc}")

    tables = info.get("tables", [])
    payload = {
        "dataset_ref": info.get("dataset_ref", ""),
        "resolved_dataset": resolved_hint,
        "requested_dataset": dataset_hint,
        "tables": tables,
    }
    if fallback_note:
        payload["note"] = fallback_note
    return {
        "ok": True,
        "payload": payload,
        "error": None,
        "artifacts": [
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
    }


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

    _, dataset = rbac.project_from_table_ref(table_ref)
    allowed, reason = rbac.check_dataset(context.get("user"), dataset)
    if not allowed:
        return _err(f"RBAC: {reason}")

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

def _looks_trivial_sql(sql: str) -> bool:
    """Detecta SQL "placeholder" (sem FROM real, ou mensagem de erro embutida)."""
    if not sql:
        return True
    norm = re.sub(r"\s+", " ", sql.strip().rstrip(";").lower())
    # Sem FROM referenciando tabela real → trivial.
    if not re.search(r"\bfrom\s+[`\w]", norm):
        return True
    # Mensagem de erro/placeholder no SELECT (heurística).
    if re.search(r"select\s+'[^']*(?:erro|nao\s+(?:foi)?\s*possivel|placeholder)[^']*'\s+as\s+\w", norm):
        return True
    return False


def _validate_and_run_sql(
    sql: str,
    project_id: str,
    max_rows: int,
    user: dict[str, Any] | None = None,
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
    if _looks_trivial_sql(sql):
        return _err(
            "SQL gerado é trivial (sem FROM real ou apenas placeholder). "
            "Provavelmente os schemas das tabelas relevantes não foram coletados."
        )

    # RBAC: cada dataset citado precisa estar permitido para o usuário.
    referenced = set(re.findall(r"`?([\w\-]+)\.([\w\-]+)\.[\w\-]+`?", sql))
    for _proj, dataset in referenced:
        allowed, reason = rbac.check_dataset(user, dataset)
        if not allowed:
            return _err(f"RBAC: {reason}")

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
        user=context.get("user"),
    )


# ---------------------------------------------------------------------------
# text_to_sql — LLM gera SQL a partir de NL + schemas
# ---------------------------------------------------------------------------

_TEXT_TO_SQL_PROMPT = """\
Você é um gerador de SQL para BigQuery. Receberá:
1. Uma pergunta em linguagem natural.
2. Os schemas das tabelas relevantes.

Devolva APENAS uma query SELECT/WITH executável. A query deve:
- Usar APENAS as tabelas listadas, sempre com nome totalmente qualificado \
(`projeto.dataset.tabela`).
- Aplicar LIMIT compatível com o pedido (default 200) salvo se o usuário \
pedir explicitamente uma agregação.
- Evitar SELECT *: liste colunas explicitamente quando possível.
- Não usar DDL/DML (INSERT/UPDATE/DELETE/CREATE/etc).
- Não incluir comentários longos, mensagens de erro nem placeholders.
- Se não for possível responder com os dados disponíveis, devolva uma query \
mínima válida que ainda consulte a tabela mais provável (NUNCA devolva uma \
mensagem de erro em forma de string).
"""


def _extract_sql_from_llm_text(text: str) -> str:
    """Fallback de extração quando o structured output não é usado.

    Tenta, em ordem: (1) JSON puro com chave `sql`; (2) bloco ```sql``` ou
    ```json```; (3) o próprio texto. Usado apenas como safety-net.
    """
    text = (text or "").strip()
    if not text:
        return ""
    try:
        data = json.loads(text)
        if isinstance(data, dict) and isinstance(data.get("sql"), str):
            return data["sql"].strip()
    except Exception:  # noqa: BLE001
        pass
    match = _SQL_FENCE_PATTERN.search(text)
    if match:
        inner = match.group(1).strip()
        # Pode ser um JSON dentro do fence — tenta desembrulhar.
        try:
            data = json.loads(inner)
            if isinstance(data, dict) and isinstance(data.get("sql"), str):
                return data["sql"].strip()
        except Exception:  # noqa: BLE001
            pass
        return inner
    return text


_PICK_TABLES_PROMPT = """\
Você recebe uma lista de tabelas com suas colunas (em JSON) e uma pergunta \
de negócio em português. Sua tarefa é selecionar as 1-{max_pick} tabelas mais \
relevantes para responder a pergunta. Use os NOMES das tabelas e o conjunto \
de COLUNAS como evidência semântica.

Retorne SOMENTE este JSON, sem texto extra:
{{"table_ids": ["nome_da_tabela_1", "nome_da_tabela_2"], "rationale": "..."}}
"""


def _pick_relevant_tables(
    natural_language: str,
    tables: list[dict[str, Any]],
    llm: Any,
    max_pick: int = 5,
) -> list[dict[str, Any]]:
    """Filtra `tables` pelas mais relevantes à pergunta. Fallback: heurística textual."""
    if not tables:
        return []
    if len(tables) <= max_pick:
        return list(tables)

    summary = [
        {
            "table_id": t.get("table_id", ""),
            "columns": (t.get("columns") or [])[:20],
        }
        for t in tables[:80]  # cap defensivo no prompt
    ]
    try:
        from pydantic import BaseModel, Field

        class _Picked(BaseModel):
            table_ids: list[str] = Field(default_factory=list)
            rationale: str = ""

        structured = llm.with_structured_output(_Picked)
        result: _Picked = invoke_with_retry(
            structured,
            [
                SystemMessage(content=_PICK_TABLES_PROMPT.format(max_pick=max_pick)),
                HumanMessage(content=(
                    f"PERGUNTA:\n{natural_language}\n\nTABELAS DISPONÍVEIS:\n"
                    f"{json.dumps(summary, ensure_ascii=False, indent=2)}"
                )),
            ],
            max_attempts=2,
        )
        ids = {tid.strip() for tid in (result.table_ids or []) if tid}
        if ids:
            picked = [t for t in tables if t.get("table_id") in ids]
            if picked:
                return picked[:max_pick]
    except Exception:  # noqa: BLE001
        pass

    # Fallback heurístico: token overlap entre pergunta e (table_id + columns).
    import unicodedata as _u

    def _toks(text: str) -> set[str]:
        s = "".join(c for c in _u.normalize("NFD", text or "") if _u.category(c) != "Mn")
        return {tok for tok in re.findall(r"[a-z0-9_]+", s.lower()) if len(tok) > 2}

    q = _toks(natural_language)
    scored = []
    for t in tables:
        haystack = (t.get("table_id") or "") + " " + " ".join(t.get("columns") or [])
        score = len(q.intersection(_toks(haystack)))
        if score > 0:
            scored.append((score, t))
    scored.sort(key=lambda kv: kv[0], reverse=True)
    return [t for _, t in scored[:max_pick]] or tables[:max_pick]


def cap_text_to_sql(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    natural_language = str(args.get("natural_language") or context.get("request_text") or "").strip()
    if not natural_language:
        return _err("natural_language ausente.")

    table_refs = [str(t).strip() for t in (args.get("table_refs") or []) if str(t).strip()]
    dataset_ref = str(args.get("dataset_ref") or "").strip()

    llm = context.get("llm")
    if llm is None:
        return _err("LLM indisponível neste contexto.")

    project_id = (context.get("project_id") or "").strip()
    if not project_id and table_refs:
        project_id = _resolve_project_for_table(table_refs[0], None)
    if not project_id and dataset_ref:
        parts = dataset_ref.split(".")
        if len(parts) == 2:
            project_id = parts[0]
    if not project_id:
        return _err("project_id ausente para text_to_sql.")

    auto_picked_note = ""

    # Caminho autônomo: dataset_ref informado e sem table_refs → descobre + escolhe.
    if not table_refs and dataset_ref:
        parts = dataset_ref.split(".")
        if len(parts) == 2:
            dataset_project, dataset_id = parts
        elif len(parts) == 1:
            dataset_project, dataset_id = project_id, parts[0]
        else:
            return _err("dataset_ref inválido. Use 'projeto.dataset' ou 'dataset'.")

        allowed, reason = rbac.check_dataset(context.get("user"), dataset_id)
        if not allowed:
            return _err(f"RBAC: {reason}")

        try:
            info = get_dataset_tables_metadata(dataset_project, dataset_id, max_tables=80, max_columns=20)
        except Exception as exc:  # noqa: BLE001
            return _err(f"Falha ao listar tabelas de {dataset_project}.{dataset_id}: {exc}")
        all_tables = info.get("tables") or []
        if not all_tables:
            return _err(f"Dataset {dataset_project}.{dataset_id} não tem tabelas.")

        picked = _pick_relevant_tables(natural_language, all_tables, llm, max_pick=5)
        if not picked:
            return _err("Não foi possível identificar tabelas relevantes para a pergunta.")
        table_refs = [
            t.get("full_name") or f"{dataset_project}.{dataset_id}.{t.get('table_id')}"
            for t in picked
        ]
        auto_picked_note = (
            f"Tabelas selecionadas automaticamente em {dataset_project}.{dataset_id}: "
            f"{', '.join(t.get('table_id', '') for t in picked)}."
        )

    if not table_refs:
        return _err(
            "Informe `table_refs` (lista qualificada) OU `dataset_ref` para descoberta automática."
        )

    # Validação + RBAC por tabela.
    for ref in table_refs:
        if not _TABLE_REF_PATTERN.match(ref):
            return _err(f"table_ref inválido: {ref}")
        _, dataset = rbac.project_from_table_ref(ref)
        allowed, reason = rbac.check_dataset(context.get("user"), dataset)
        if not allowed:
            return _err(f"RBAC: {reason}")

    # 1) Coleta schemas das tabelas escolhidas.
    schemas: list[str] = []
    for ref in table_refs[:8]:
        try:
            schemas.append(get_table_schema(ref, _resolve_project_for_table(ref, project_id), max_columns=50))
        except Exception as exc:  # noqa: BLE001
            schemas.append(f"[Falha ao obter schema de {ref}: {exc}]")
    schemas_text = "\n\n".join(schemas) or "(sem schemas)"

    # 2) Gera SQL via LLM (structured output → garante {sql: str} válido).
    from pydantic import BaseModel, Field

    class _SqlOutput(BaseModel):
        sql: str = Field(..., description="Apenas o SQL SELECT/WITH, sem markdown.")

    user_msg = (
        f"PERGUNTA:\n{natural_language}\n\n"
        f"SCHEMAS DISPONÍVEIS:\n{schemas_text}"
    )
    sql = ""
    try:
        structured_llm = llm.with_structured_output(_SqlOutput)
        result: _SqlOutput = invoke_with_retry(
            structured_llm,
            [SystemMessage(content=_TEXT_TO_SQL_PROMPT), HumanMessage(content=user_msg)],
            max_attempts=2,
        )
        if result and getattr(result, "sql", None):
            sql = str(result.sql).strip().strip("`").strip()
    except Exception as exc:  # noqa: BLE001
        # Fallback: invocação plana e parsing tolerante.
        try:
            response = invoke_with_retry(
                llm,
                [SystemMessage(content=_TEXT_TO_SQL_PROMPT), HumanMessage(content=user_msg)],
                max_attempts=1,
            )
            sql = _extract_sql_from_llm_text(_llm_text(response))
        except Exception as exc2:  # noqa: BLE001
            return _err(f"Falha ao gerar SQL via LLM: {exc2 or exc}")

    if not sql:
        return _err("LLM não devolveu SQL utilizável.")

    # 3) Valida e executa.
    row_limit = int(args.get("row_limit") or _DEFAULT_BQ_QUERY_MAX_ROWS)
    exec_result = _validate_and_run_sql(
        sql=sql, project_id=project_id, max_rows=row_limit, user=context.get("user")
    )
    if exec_result["ok"]:
        exec_result["payload"]["natural_language"] = natural_language
        exec_result["payload"]["table_refs"] = table_refs
        if auto_picked_note:
            exec_result["payload"]["auto_picked_note"] = auto_picked_note
    else:
        exec_result["payload"] = {
            "attempted_sql": sql,
            "natural_language": natural_language,
            "table_refs": table_refs,
        }
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
# metric_lookup — busca no Semantic Layer
# ---------------------------------------------------------------------------

def cap_metric_lookup(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    query = str(args.get("query") or context.get("request_text") or "").strip()
    if not query:
        return _err("query ausente para metric_lookup.")
    try:
        top_k = int(args.get("top_k") or 5)
    except (TypeError, ValueError):
        top_k = 5
    matches = semantic_layer.search_metrics(query, top_k=max(1, min(top_k, 20)))
    # Filtra por RBAC.
    user = context.get("user")
    visible: list[dict[str, Any]] = []
    for m in matches:
        ok, _ = rbac.check_metric(user, m.get("key", ""))
        if ok:
            visible.append(m)
    rows = [
        {
            "key": m.get("key", ""),
            "name": m.get("name", ""),
            "description": (m.get("description") or "")[:200],
            "source_table": m.get("source_table", ""),
            "tags": m.get("tags", ""),
        }
        for m in visible
    ]
    return _ok(
        payload={"query": query, "matches": rows, "match_count": len(rows)},
        artifacts=[
            {
                "type": "table",
                "title": f"Métricas governadas (busca: {query})",
                "columns": ["key", "name", "description", "source_table", "tags"],
                "rows": rows,
            }
        ] if rows else [],
    )


# ---------------------------------------------------------------------------
# metric_execute — executa uma métrica registrada
# ---------------------------------------------------------------------------

def cap_metric_execute(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    key = str(args.get("key") or "").strip()
    if not key:
        return _err("key ausente para metric_execute.")
    allowed, reason = rbac.check_metric(context.get("user"), key)
    if not allowed:
        return _err(f"RBAC: {reason}")
    metric = semantic_layer.resolve_metric(key)
    if not metric:
        return _err(f"Métrica '{key}' não encontrada no Semantic Layer.")
    sql, params_used = semantic_layer.render_sql(
        metric.get("sql_template", ""), args.get("params") or {}
    )
    if not sql:
        return _err(f"Métrica '{key}' está sem sql_template.")
    project_id = (context.get("project_id") or "").strip()
    if not project_id:
        return _err("project_id ausente para executar métrica.")
    row_limit = int((args.get("params") or {}).get("limit") or _DEFAULT_BQ_QUERY_MAX_ROWS)
    exec_result = _validate_and_run_sql(
        sql=sql, project_id=project_id, max_rows=row_limit, user=context.get("user")
    )
    # Enriquecimento informativo no payload, independente do sucesso.
    payload = exec_result.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {"value": payload}
    payload["metric_key"] = key
    payload["metric_name"] = metric.get("name", "")
    payload["params_used"] = params_used
    exec_result["payload"] = payload
    return exec_result


# ---------------------------------------------------------------------------
# org_fact_save / org_fact_recall — memória organizacional
# ---------------------------------------------------------------------------

def _user_id_from_context(context: dict[str, Any]) -> str:
    u = context.get("user") or {}
    return str(u.get("username") or u.get("user_id") or "").strip()


def cap_org_fact_save(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    user_id = _user_id_from_context(context)
    if not user_id:
        return _err("Sem usuário autenticado para salvar memória.")
    fact = str(args.get("fact_text") or "").strip()
    if not fact:
        return _err("fact_text vazio.")
    tags = str(args.get("tags") or "")
    scope = str(args.get("scope") or "user").strip().lower()
    if scope not in {"user", "global"}:
        scope = "user"
    # 'global' só admin
    if scope == "global" and not (context.get("user") or {}).get("is_admin"):
        scope = "user"
    fact_id = org_memory.save_fact(user_id=user_id, fact_text=fact, tags=tags, scope=scope)
    if not fact_id:
        return _err("Falha ao salvar fato.")
    return _ok(payload={"id": fact_id, "scope": scope, "fact_text": fact})


def cap_org_fact_recall(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    user_id = _user_id_from_context(context)
    query = str(args.get("query") or context.get("request_text") or "").strip()
    try:
        top_k = int(args.get("top_k") or 5)
    except (TypeError, ValueError):
        top_k = 5
    facts = org_memory.recall(user_id=user_id, query=query, top_k=max(1, min(top_k, 20)))
    return _ok(
        payload={"query": query, "facts": facts, "fact_count": len(facts)},
        artifacts=[
            {
                "type": "table",
                "title": "Memória organizacional",
                "columns": ["id", "scope", "fact_text", "tags", "created_at"],
                "rows": facts,
            }
        ] if facts else [],
    )


# ---------------------------------------------------------------------------
# forecast_simple — regressão linear sobre rows de step anterior
# ---------------------------------------------------------------------------

def cap_forecast_simple(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    rows, err = _resolve_source_rows(args, context)
    if err:
        return _err(err)
    value_column = str(args.get("value_column") or "").strip()
    if not value_column:
        return _err("value_column é obrigatório.")
    time_column = str(args.get("time_column") or "").strip() or None
    try:
        horizon = int(args.get("horizon") or 6)
    except (TypeError, ValueError):
        horizon = 6
    result = forecast_mod.project(
        rows=rows, value_column=value_column, horizon=horizon, time_column=time_column
    )
    if not result.get("ok"):
        return _err(result.get("error") or "Falha no forecast.")
    return _ok(
        payload=result,
        artifacts=[
            {
                "type": "table",
                "title": f"Forecast ({value_column}, horizonte={horizon})",
                "columns": ["step", "x", "y"],
                "rows": result["forecasts"],
            }
        ],
    )


# ---------------------------------------------------------------------------
# attachment_analyze — CSV (stdlib) ou imagem (Gemini multimodal)
# ---------------------------------------------------------------------------

def cap_attachment_analyze(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    attachments = context.get("attachments") or []
    if not attachments:
        return _err("Nenhum anexo foi enviado nesta requisição.")
    try:
        idx = int(args.get("attachment_index") or 0)
    except (TypeError, ValueError):
        return _err("attachment_index inválido.")
    if not (0 <= idx < len(attachments)):
        return _err(f"attachment_index fora do intervalo (0..{len(attachments) - 1}).")
    att = attachments[idx] or {}
    kind = str(att.get("kind") or "").strip().lower()
    data = str(att.get("data") or "")
    if kind not in multimodal.VALID_KINDS:
        return _err(f"kind inválido: '{kind}'. Use {sorted(multimodal.VALID_KINDS)}.")
    if not data:
        return _err("Anexo sem campo 'data' (base64).")

    if kind == multimodal.KIND_CSV:
        try:
            parsed = multimodal.parse_csv(data, delimiter=att.get("delimiter"))
        except ValueError as exc:
            return _err(f"Falha ao ler CSV: {exc}")
        return _ok(
            payload={
                "kind": kind,
                "filename": att.get("filename") or "",
                "row_count": parsed["row_count"],
                "columns": parsed["columns"],
                "rows": parsed["rows"],
                "delimiter": parsed["delimiter"],
                "truncated": parsed["truncated"],
            },
            artifacts=[
                {
                    "type": "table",
                    "title": f"CSV anexo: {att.get('filename') or f'#{idx}'}",
                    "columns": parsed["columns"],
                    "rows": parsed["rows"],
                }
            ],
        )

    # KIND_IMAGE
    prompt = str(args.get("prompt") or "Descreva o conteúdo desta imagem em PT-BR.")
    try:
        description = multimodal.describe_image_with_llm(
            b64=data,
            prompt=prompt,
            llm=context.get("llm_creative") or context.get("llm"),
            mime_type=att.get("mime_type") or "image/png",
        )
    except ValueError as exc:
        return _err(f"Falha ao analisar imagem: {exc}")
    except Exception as exc:  # noqa: BLE001
        return _err(f"LLM multimodal falhou: {exc}")
    return _ok(
        payload={
            "kind": kind,
            "filename": att.get("filename") or "",
            "description": description,
        },
        artifacts=[
            {
                "type": "schema",
                "table_ref": att.get("filename") or f"imagem#{idx}",
                "text": description,
            }
        ],
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
    CAPABILITY_METRIC_LOOKUP: cap_metric_lookup,
    CAPABILITY_METRIC_EXECUTE: cap_metric_execute,
    CAPABILITY_ORG_FACT_SAVE: cap_org_fact_save,
    CAPABILITY_ORG_FACT_RECALL: cap_org_fact_recall,
    CAPABILITY_FORECAST_SIMPLE: cap_forecast_simple,
    CAPABILITY_ATTACHMENT_ANALYZE: cap_attachment_analyze,
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
    "cap_metric_lookup",
    "cap_metric_execute",
    "cap_org_fact_save",
    "cap_org_fact_recall",
    "cap_forecast_simple",
    "cap_attachment_analyze",
    "cap_chat_answer",
]
