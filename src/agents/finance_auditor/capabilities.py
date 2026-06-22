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

from src.agents.finance_auditor import catalog_index, forecast as forecast_mod
from src.agents.finance_auditor import multimodal, org_memory, rbac, semantic_layer
from src.agents.finance_auditor.supervisor_schemas import (
    CAPABILITY_ATTACHMENT_ANALYZE,
    CAPABILITY_BQ_GET_SCHEMA,
    CAPABILITY_BQ_LIST_DATASETS,
    CAPABILITY_BQ_LIST_TABLES,
    CAPABILITY_BQ_QUERY,
    CAPABILITY_CATALOG_SEARCH,
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
    get_table_column_types,
    get_table_schema,
    list_datasets_with_labels,
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


def _fuzzy_pick_column(hint: str, columns: list[str]) -> str | None:
    """Mesma lógica de `_fuzzy_pick_dataset`, mas para nome de coluna.

    O Planner monta `viz_spec`/`stats_describe` no mesmo turno em que pediu
    o `text_to_sql` — sem nunca ter visto o schema real da query que ele
    mesmo gerou — e frequentemente chuta o nome da coluna (case errado,
    abreviação, acento). Threshold de similaridade mais alto que o de
    dataset (0.6 vs 0.4): colunas da MESMA tabela podem ser parecidas entre
    si e semanticamente opostas (ex.: "valor_atual" vs "valor_anterior").
    """
    if not hint or not columns:
        return None
    hint_slug = _slug(hint)
    if not hint_slug:
        return None

    for col in columns:
        if _slug(col) == hint_slug:
            return col
    for col in columns:
        col_slug = _slug(col)
        if hint_slug in col_slug or col_slug in hint_slug:
            return col
    scored = sorted(
        ((difflib.SequenceMatcher(None, hint_slug, _slug(col)).ratio(), col) for col in columns),
        reverse=True,
    )
    if scored and scored[0][0] >= 0.6:
        return scored[0][1]
    return None


def _list_project_datasets(project_id: str) -> list[str]:
    from src.shared.tools.bigquery import _get_client  # noqa: WPS437

    client = _get_client(project_id)
    return [ds.dataset_id for ds in client.list_datasets(project_id)]


def resolve_dataset_by_gerencia(project_id: str, gerencia_hint: str) -> dict[str, str] | None:
    """Resolve o dataset cujo rotulo (label) do BigQuery corresponde à gerência.

    A chave do rótulo é configurável (`FINANCE_AUDITOR_GERENCIA_LABEL_KEY`,
    default "gerencia") — o valor é casado via `_fuzzy_pick_dataset` (mesma
    lógica de match exato → substring → similaridade já usada para nomes de
    dataset), aplicada aos *valores* do rótulo em vez dos *nomes* dos
    datasets, já que o nome de um dataset pode não ter nenhuma relação
    textual com a gerência à qual ele pertence.
    """
    if not gerencia_hint or not gerencia_hint.strip():
        return None
    label_key = get_runtime_config("FINANCE_AUDITOR_GERENCIA_LABEL_KEY", "gerencia")
    try:
        datasets = list_datasets_with_labels(project_id)
    except Exception:  # noqa: BLE001
        return None

    value_to_dataset: dict[str, str] = {}
    for ds in datasets:
        value = str((ds.get("labels") or {}).get(label_key) or "").strip()
        if value and value not in value_to_dataset:
            value_to_dataset[value] = ds["dataset_id"]

    picked_value = _fuzzy_pick_dataset(gerencia_hint, list(value_to_dataset.keys()))
    if not picked_value:
        return None
    return {
        "dataset_id": value_to_dataset[picked_value],
        "gerencia": picked_value,
        "label_key": label_key,
    }


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
- Ao filtrar por período relativo ("últimos N dias/meses/anos"), confira o \
tipo da coluna de data/hora no schema antes de escrever o filtro: \
`TIMESTAMP_SUB`/`TIMESTAMP_ADD` NÃO aceitam MONTH/QUARTER/YEAR (só DAY, \
HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND), e o BigQuery não compara \
`DATE` com `TIMESTAMP`/`DATETIME` sem conversão explícita. Para evitar os \
dois erros de uma vez, normalize a coluna com `DATE(coluna)` (funciona para \
TIMESTAMP, DATETIME ou DATE) e compare com `DATE_SUB(CURRENT_DATE(), \
INTERVAL N MONTH)` — essa forma aceita qualquer unidade e nunca dá erro de \
tipo incompatível.
- Não usar DDL/DML (INSERT/UPDATE/DELETE/CREATE/etc).
- Não incluir comentários longos, mensagens de erro nem placeholders.
- Se não for possível responder com os dados disponíveis, devolva uma query \
mínima válida que ainda consulte a tabela mais provável (NUNCA devolva uma \
mensagem de erro em forma de string).
- Se a entrada incluir uma seção "TENTATIVA ANTERIOR FALHOU" com SQL e erro \
do BigQuery, sua prioridade é corrigir EXATAMENTE essa causa — leia a \
mensagem de erro com atenção e ajuste só o que ela aponta, em vez de gerar \
uma query genérica do zero (que tende a repetir o mesmo erro ou trocar por \
outro da mesma natureza).
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

    # Preenchidos automaticamente pelo router (`_attach_retry_feedback`) quando
    # este step é um retry pós-Reflect de um text_to_sql que falhou — sem
    # isso, a "autocorreção" seria as cegas: o LLM regeneraria do zero sem
    # saber qual SQL já foi tentada nem por que o BigQuery a rejeitou.
    previous_sql = str(args.get("previous_sql") or "").strip()
    previous_error = str(args.get("previous_error") or "").strip()

    table_refs = [str(t).strip() for t in (args.get("table_refs") or []) if str(t).strip()]
    dataset_ref = str(args.get("dataset_ref") or "").strip()
    if not table_refs and not dataset_ref:
        # Fallback: dataset já fixado no contexto (ex.: gerência pinada na
        # sessão) — evita exigir que o Planner sempre informe dataset_ref.
        dataset_ref = str(context.get("dataset_hint") or "").strip()

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
            # Mesma autocorreção do bq_list_tables: se o dataset não existe,
            # lista os datasets reais do projeto e tenta fuzzy match.
            msg = str(exc).lower()
            if "not found" in msg or "notfound" in msg or "404" in msg:
                try:
                    available = _list_project_datasets(dataset_project)
                except Exception:  # noqa: BLE001
                    return _err(
                        f"Dataset '{dataset_id}' não existe e não foi possível "
                        f"listar o projeto."
                    )
                pick = _fuzzy_pick_dataset(dataset_id, available)
                if not pick:
                    return _err(
                        f"Não encontrei um dataset compatível com '{dataset_id}' "
                        f"no projeto."
                    )
                # Re-checa RBAC para o dataset substituto.
                allowed_pick, reason_pick = rbac.check_dataset(context.get("user"), pick)
                if not allowed_pick:
                    return _err(f"RBAC: dataset substituto '{pick}' negado ({reason_pick}).")
                try:
                    info = get_dataset_tables_metadata(dataset_project, pick, max_tables=80, max_columns=20)
                    dataset_id = pick
                except Exception as exc2:  # noqa: BLE001
                    return _err(f"Falha ao listar tabelas de {dataset_project}.{pick}: {exc2}")
            else:
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

    # Caminho RAG: nem table_refs nem dataset_ref informados — busca as
    # tabelas certas por SIGNIFICADO no índice do catálogo, em vez de exigir
    # que o Planner já saiba (ou chute) o nome do dataset.
    elif not table_refs and not dataset_ref:
        matches = catalog_index.search_catalog(project_id, natural_language, top_k=5)
        allowed_matches = []
        for m in matches:
            allowed, _reason = rbac.check_dataset(context.get("user"), m["dataset_id"])
            if allowed:
                allowed_matches.append(m)
        if not allowed_matches:
            return _err(
                "Não encontrei tabelas relevantes para essa pergunta no catálogo. "
                "Informe `table_refs` ou `dataset_ref` explicitamente."
            )
        table_refs = [m["full_name"] for m in allowed_matches]
        auto_picked_note = (
            "Tabelas selecionadas automaticamente por busca semântica no catálogo: "
            f"{', '.join(m['table_id'] for m in allowed_matches)}."
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
    if previous_sql and previous_error:
        user_msg += (
            "\n\nTENTATIVA ANTERIOR FALHOU — corrija especificamente este "
            f"problema, não repita o mesmo padrão:\nSQL anterior:\n{previous_sql}\n\n"
            f"Erro retornado pelo BigQuery:\n{previous_error}"
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

    available_cols = list(rows[0].keys())
    requested = [str(c) for c in (args.get("columns") or [])]
    if requested:
        # Mesmo chute de nome de coluna do viz_spec (ver _fuzzy_pick_column)
        # — resolve o que der; se nenhuma bater, cai para todas as colunas
        # em vez de devolver estatística vazia silenciosamente.
        resolved = [
            c if c in rows[0] else _fuzzy_pick_column(c, available_cols)
            for c in requested
        ]
        keys = [c for c in resolved if c is not None] or available_cols
    else:
        keys = available_cols

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

# Paleta de marca (Porto Seguro) com extensões semânticas — usada tanto para
# a cor única de marks quanto para o range categórico (color encoding).
_CHART_PALETTE = [
    "#004691",  # porto-primary
    "#00a1e4",  # porto-vivid
    "#059669",  # emerald
    "#d97706",  # amber
    "#6d28d9",  # violet
    "#0891b2",  # teal
    "#be123c",  # rose
    "#64748b",  # slate (overflow de categorias)
]

# Nota: o locale pt-BR de numero/data ("1.234,56", "jan/2026") e setado
# globalmente no frontend via vega.formatLocale/vega.timeFormatLocale (ver
# scripts.js, _faEnsureVegaLocale) -- nao existe propriedade "locale" no
# nivel raiz de um spec Vega-Lite, entao nao tem como (nem precisa) ir aqui.

# Tema visual aplicado a todo gráfico gerado: tipografia da marca (Sora/DM
# Sans), grid sutil só no eixo Y, eixos sem moldura pesada e paleta de marca
# — visual de terminal financeiro em vez do tema cru padrão do Vega-Lite.
_VEGA_CONFIG = {
    "background": "transparent",
    "font": "DM Sans, sans-serif",
    "title": {
        "font": "Sora, sans-serif",
        "fontSize": 13,
        "fontWeight": 600,
        "color": "#0a1628",
        "subtitleColor": "#8096b2",
        "subtitleFontSize": 10.5,
        "subtitleFontWeight": 500,
        "anchor": "start",
        "offset": 14,
    },
    "axis": {
        "labelFont": "DM Sans, sans-serif",
        "labelFontSize": 10.5,
        "labelColor": "#3d5276",
        "labelPadding": 6,
        "titleFont": "DM Sans, sans-serif",
        "titleFontSize": 11,
        "titleFontWeight": 600,
        "titleColor": "#3d5276",
        "titlePadding": 12,
        "domainColor": "#d1dce7",
        "tickColor": "#d1dce7",
        "tickSize": 4,
        "gridColor": "#eef2f7",
        "gridDash": [3, 3],
    },
    "axisY": {"grid": True, "domain": False, "ticks": False},
    "axisX": {"grid": False},
    "legend": {
        "labelFont": "DM Sans, sans-serif",
        "labelFontSize": 10.5,
        "labelColor": "#3d5276",
        "titleFont": "DM Sans, sans-serif",
        "titleFontSize": 11,
        "titleColor": "#0a1628",
        "symbolType": "circle",
        "symbolSize": 70,
        "orient": "top",
        "direction": "horizontal",
        "offset": 10,
        "padding": 4,
    },
    "view": {"stroke": "transparent"},
    "range": {"category": _CHART_PALETTE},
    "bar": {"color": "#004691", "cornerRadiusTopLeft": 4, "cornerRadiusTopRight": 4},
    "line": {"color": "#004691", "strokeWidth": 2.5},
    "point": {"color": "#004691", "filled": True, "size": 64},
    "area": {"color": "#004691", "line": True, "opacity": 0.22},
    "arc": {"stroke": "#ffffff", "strokeWidth": 2},
}


def _humanize_field(name: str) -> str:
    """"vlr_receita_mensal" -> "Vlr receita mensal" — título de eixo legível
    sem precisar que o Planner descreva cada coluna."""
    cleaned = re.sub(r"[_\-]+", " ", str(name)).strip()
    return f"{cleaned[:1].upper()}{cleaned[1:]}" if cleaned else str(name)


def _humanize_bool_columns(rows: list[dict[str, Any]], columns: list[str]) -> list[dict[str, Any]]:
    """Coluna 100% booleana (True/False) aparece crua como "true"/"false" na
    legenda e no tooltip — vira "Sim"/"Não", sem alterar o que o dado representa."""
    bool_cols = {
        col
        for col in columns
        if (values := [r.get(col) for r in rows if r.get(col) is not None]) and all(isinstance(v, bool) for v in values)
    }
    if not bool_cols:
        return rows
    return [
        {k: ("Sim" if v is True else "Não" if v is False else v) if k in bool_cols else v for k, v in r.items()}
        for r in rows
    ]


def _number_format(values: list[Any]) -> str:
    """Formato de eixo (combinado com o locale pt-BR setado no frontend):
    abrevia números grandes (1,2M) pra não estourar o espaço do gráfico —
    nunca assume moeda/unidade, só ajusta separador de milhar/abreviação
    para o que os dados de fato têm."""
    nums = [_to_float(v) for v in values if _is_number(v)]
    if not nums:
        return ",.0f"
    if max(abs(v) for v in nums) >= 1_000_000:
        return ".2~s"
    has_decimals = any(abs(v - round(v)) > 1e-9 for v in nums)
    return ",.2f" if has_decimals else ",.0f"


def _tooltip_number_format(values: list[Any]) -> str:
    """Formato de tooltip: nunca abrevia — é exatamente ao passar o mouse que
    o usuário quer o valor exato, não a aproximação "1,2M" do eixo."""
    nums = [_to_float(v) for v in values if _is_number(v)]
    if not nums:
        return ",.0f"
    has_decimals = any(abs(v - round(v)) > 1e-9 for v in nums)
    return ",.2f" if has_decimals else ",.0f"


def _date_precision_format(values: list[Any]) -> tuple[str, str]:
    """(formato_eixo, formato_tooltip) conforme granularidade real da coluna
    temporal — dia (YYYY-MM-DD) vs mês (YYYY-MM)."""
    sample = [v for v in values if isinstance(v, str)][:50]
    day_level = any(len(v) >= 10 for v in sample)
    return ("%d/%b", "%d/%m/%Y") if day_level else ("%b/%Y", "%b/%Y")


def _infer_field_type(values: list[Any]) -> str:
    sample = [v for v in values if v is not None][:50]
    if not sample:
        return "nominal"
    if all(_is_number(v) for v in sample):
        return "quantitative"
    # Aceita "YYYY-MM-DD" (e variantes com hora) e também "YYYY-MM" puro —
    # comum em consultas de evolução mensal (ex.: `FORMAT_DATE('%Y-%m', ...)`),
    # que sem isso caía em "nominal" e quebrava a leitura temporal do gráfico.
    if all(isinstance(v, str) and re.match(r"^\d{4}-\d{2}(-\d{2})?", v) for v in sample):
        return "temporal"
    return "nominal"


def _suggest_chart_type(x_type: str, y_type: str) -> str:
    """Heurística de escolha de gráfico quando o Planner não informa `chart_type`.

    Deliberadamente nunca sugere `arc` (pizza): exige leitura semântica de
    "parte de um todo" que não dá para inferir só do tipo das colunas — fica
    reservado para quando o Planner pede explicitamente.
    """
    if x_type == "temporal":
        return "line"
    if x_type == "quantitative" and y_type == "quantitative":
        return "point"
    return "bar"


def cap_viz_spec(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    rows, err = _resolve_source_rows(args, context)
    if err:
        return _err(err)
    if not rows:
        return _err("Sem linhas para gerar gráfico.")

    x_arg = str(args.get("x") or "").strip()
    y_arg = str(args.get("y") or "").strip()
    if not x_arg or not y_arg:
        return _err("x e y são obrigatórios.")

    available_cols = list(rows[0].keys())
    # Tolera nome de coluna "chutado" (case/acento/abreviação) — ver
    # docstring de _fuzzy_pick_column. Se ainda assim não resolver, a
    # mensagem de erro lista as colunas reais, para o Planner corrigir no
    # replan em vez de tentar de novo às cegas.
    x = x_arg if x_arg in rows[0] else _fuzzy_pick_column(x_arg, available_cols)
    y = y_arg if y_arg in rows[0] else _fuzzy_pick_column(y_arg, available_cols)
    if x is None or y is None:
        unresolved = ", ".join(
            f"{label}={arg!r}"
            for label, arg, resolved in (("x", x_arg, x), ("y", y_arg, y))
            if resolved is None
        )
        return _err(
            f"{unresolved} não corresponde a nenhuma coluna do step de origem. "
            f"Colunas disponíveis: {', '.join(available_cols)}."
        )

    color_arg = str(args.get("color") or "").strip()
    color = (color_arg if color_arg in rows[0] else _fuzzy_pick_column(color_arg, available_cols)) if color_arg else None
    title = str(args.get("title") or "").strip()

    rows = _humanize_bool_columns(rows, [c for c in (x, y, color) if c])

    x_type = _infer_field_type([r.get(x) for r in rows])
    y_type = _infer_field_type([r.get(y) for r in rows])

    requested_chart_type = str(args.get("chart_type") or "").strip().lower()
    auto_selected = not bool(requested_chart_type)
    chart_type = requested_chart_type or _suggest_chart_type(x_type, y_type)
    if chart_type not in _VALID_CHART_TYPES:
        return _err(f"chart_type inválido: {chart_type}. Use um de {sorted(_VALID_CHART_TYPES)}.")

    x_title = _humanize_field(x)
    y_title = _humanize_field(y)
    # Eixo abrevia números grandes (1,2M) pra caber no espaço do gráfico;
    # tooltip sempre mostra o valor exato — é justamente o que o usuário
    # quer ao passar o mouse, não a mesma aproximação do eixo.
    y_axis_fmt = _number_format([r.get(y) for r in rows]) if y_type == "quantitative" else None
    y_tooltip_fmt = _tooltip_number_format([r.get(y) for r in rows]) if y_type == "quantitative" else None
    x_axis_num_fmt = _number_format([r.get(x) for r in rows]) if x_type == "quantitative" else None
    x_tooltip_num_fmt = _tooltip_number_format([r.get(x) for r in rows]) if x_type == "quantitative" else None
    x_axis_date_fmt = x_tooltip_date_fmt = None
    if x_type == "temporal":
        x_axis_date_fmt, x_tooltip_date_fmt = _date_precision_format([r.get(x) for r in rows])

    if chart_type == "arc":
        # Pizza/rosca: o mark "arc" não tem canais x/y — é "theta" (ângulo,
        # proporcional ao valor) + "color" (categoria) que o Vega-Lite espera.
        # O código antigo mandava x/y como em barra/linha, o que nunca
        # desenhava uma pizza de fato (sem "theta" o arco cobre o círculo
        # inteiro com um valor só).
        encoding: dict[str, Any] = {
            "theta": {"field": y, "type": "quantitative", "title": y_title, "stack": True},
            # Legenda do tema global é horizontal no topo (ok para 2-4 séries
            # de bar/line/area) — mas pizza costuma ter mais categorias e
            # cortava texto ("Sudeste", "Sul") por falta de espaço horizontal.
            # À direita, em coluna, escala para qualquer nº de categorias.
            "color": {"field": x, "type": "nominal", "title": x_title, "legend": {"orient": "right", "columns": 1}},
            "tooltip": [
                {"field": x, "type": "nominal", "title": x_title},
                {"field": y, "type": "quantitative", "title": y_title, **({"format": y_tooltip_fmt} if y_tooltip_fmt else {})},
            ],
        }
        mark: dict[str, Any] = {
            "type": "arc",
            "innerRadius": 68,
            "cornerRadius": 2,
        }
    else:
        x_axis: dict[str, Any] = {}
        if x_axis_date_fmt:
            x_axis["format"] = x_axis_date_fmt
        elif x_axis_num_fmt:
            x_axis["format"] = x_axis_num_fmt
        if chart_type == "bar" and x_type == "nominal" and len({r.get(x) for r in rows}) > 8:
            # Muitas categorias num bar chart: rótulos horizontais colidem —
            # inclina e limita a largura em vez de deixar texto truncado/sobreposto.
            x_axis["labelAngle"] = -35
            x_axis["labelLimit"] = 120

        x_tooltip_fmt = x_tooltip_date_fmt or x_tooltip_num_fmt
        tooltip_fields: list[dict[str, Any]] = [
            {
                "field": x,
                "type": x_type,
                "title": x_title,
                **({"format": x_tooltip_fmt} if x_tooltip_fmt else {}),
            },
            {"field": y, "type": y_type, "title": y_title, **({"format": y_tooltip_fmt} if y_tooltip_fmt else {})},
        ]
        # Sem título de eixo: o card já mostra o título do gráfico no cabeçalho
        # e o nome técnico da coluna ("mes_referencia") não agrega nada visto
        # dobrado debaixo do eixo — os valores dos ticks já bastam. O nome
        # legível continua disponível no tooltip/legenda.
        encoding = {
            "x": {"field": x, "type": x_type, "axis": {**x_axis, "title": None}},
            "y": {"field": y, "type": y_type, "axis": {"title": None, **({"format": y_axis_fmt} if y_axis_fmt else {})}},
            "tooltip": tooltip_fields,
        }
        if color:
            color_type = _infer_field_type([r.get(color) for r in rows])
            color_title = _humanize_field(color)
            encoding["color"] = {"field": color, "type": color_type, "title": color_title}
            tooltip_fields.append({"field": color, "type": color_type, "title": color_title})
        elif chart_type == "bar" and x_type == "nominal":
            # Sem série categórica explícita: colore cada barra pela própria
            # categoria do eixo X — tudo numa cor só lia como "uma massa",
            # não como categorias distintas pra comparar. Sem legenda: o
            # rótulo do próprio eixo já identifica cada barra.
            encoding["color"] = {"field": x, "type": "nominal", "legend": None}

        if chart_type == "line":
            mark = {"type": "line", "interpolate": "monotone", "strokeWidth": 2.5, "point": {"filled": True, "size": 34}}
        elif chart_type == "area":
            if color:
                mark = {"type": "area", "interpolate": "monotone", "opacity": 0.28, "line": {"strokeWidth": 2}}
            else:
                # Sem série categórica: gradiente suave da cor de marca até
                # transparente na base — leitura de "volume" mais clara do
                # que um preenchimento sólido chapado.
                mark = {
                    "type": "area",
                    "interpolate": "monotone",
                    "line": {"color": "#004691", "strokeWidth": 2.5},
                    "color": {
                        "x1": 0, "y1": 1, "x2": 0, "y2": 0,
                        "gradient": "linear",
                        "stops": [
                            {"offset": 0, "color": "rgba(0,70,145,0.02)"},
                            {"offset": 1, "color": "rgba(0,70,145,0.32)"},
                        ],
                    },
                }
        elif chart_type == "point":
            mark = {"type": "point", "filled": True, "size": 70}
        else:  # bar
            mark = {"type": "bar", "cornerRadiusTopLeft": 3, "cornerRadiusTopRight": 3}
            if x_type == "nominal":
                # Banda inteira (padrão do Vega-Lite) deixa as barras coladas
                # nas vizinhas, em bloco só — 62% da banda abre respiro real
                # entre elas.
                mark["width"] = {"band": 0.62}

    row_count = len(rows)
    display_title = title or f"{y_title} por {x_title}"

    if chart_type == "arc":
        # Pizza/rosca tem aspecto circular fixo — "width: container" estica
        # o raio pro tamanho do card e, com innerRadius fixo, vira um anel
        # fino e sem graça quando a legenda (à direita) sobra pouca largura
        # pro círculo. Tamanho fixo mantém a proporção previsível.
        size_props: dict[str, Any] = {"width": 260, "height": 260}
    else:
        # Sem título dentro do spec (o card já mostra), a altura toda vira
        # palco do gráfico — maior que antes pra aproveitar esse espaço.
        size_props = {"width": "container", "height": 320, "autosize": {"type": "fit-x", "contains": "padding"}}

    spec: dict[str, Any] = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        **size_props,
        "background": "transparent",
        "data": {"values": rows[:500]},  # cap defensivo para payload do frontend
        # Sem "title": o card de artefato (frontend) já mostra o título —
        # repetir aqui dentro do SVG só duplicava o texto e roubava espaço
        # vertical do gráfico em si.
        "mark": mark,
        "encoding": encoding,
        "config": _VEGA_CONFIG,
    }

    return _ok(
        payload={
            "chart_type": chart_type,
            "row_count": row_count,
            "title": display_title,
            "auto_selected": auto_selected,
        },
        artifacts=[{"type": "vega_lite", "title": display_title, "chart_type": chart_type, "spec": spec}],
    )


# ---------------------------------------------------------------------------
# catalog_search — RAG sobre datasets/tabelas/colunas (descoberta por significado)
# ---------------------------------------------------------------------------

def cap_catalog_search(args: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    query = str(args.get("query") or context.get("request_text") or "").strip()
    if not query:
        return _err("query ausente para catalog_search.")
    project_id = (context.get("project_id") or "").strip()
    if not project_id:
        return _err("project_id ausente para catalog_search.")
    try:
        top_k = int(args.get("top_k") or 5)
    except (TypeError, ValueError):
        top_k = 5

    matches = catalog_index.search_catalog(project_id, query, top_k=max(1, min(top_k, 20)))
    user = context.get("user")
    visible = [m for m in matches if rbac.check_dataset(user, m["dataset_id"])[0]]

    rows = [
        {
            "table_ref": m["full_name"],
            "dataset_id": m["dataset_id"],
            "table_id": m["table_id"],
            "score": m["score"],
        }
        for m in visible
    ]
    return _ok(
        payload={"query": query, "matches": rows, "match_count": len(rows)},
        artifacts=[
            {
                "type": "table",
                "title": f"Tabelas relevantes (busca: {query})",
                "columns": ["table_ref", "score"],
                "rows": rows,
            }
        ] if rows else [],
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
    # Gold Metric Catalog: quando o Planner pede gráfico/dashboard sem citar
    # uma métrica, ele restringe a busca às métricas OFICIAL=TRUE para eleger
    # a principal métrica do domínio (ver pick_gold_metric).
    official_only = bool(args.get("official_only"))
    matches = semantic_layer.search_metrics(
        query, top_k=max(1, min(top_k, 20)), official_only=official_only
    )
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
            "domain": m.get("domain", ""),
            "is_official": bool(m.get("is_official")),
        }
        for m in visible
    ]
    return _ok(
        payload={"query": query, "matches": rows, "match_count": len(rows)},
        artifacts=[
            {
                "type": "table",
                "title": f"Métricas governadas (busca: {query})",
                "columns": ["key", "name", "description", "source_table", "tags", "domain", "is_official"],
                "rows": rows,
            }
        ] if rows else [],
    )


# ---------------------------------------------------------------------------
# metric_execute — executa uma métrica registrada
# ---------------------------------------------------------------------------

# Gold Metric Catalog (sincronizado de qualquer dataset gold — ver
# catalog_index.sync_gold_metric_catalog) guarda só a EXPRESSÃO de agregação
# (ex.: "SUM(CASE WHEN DIAS_ATRASO <= 60 THEN VALOR_ABERTO END)"), não um
# SELECT completo — diferente de métrica cadastrada manualmente via admin,
# que já vem com SELECT/FROM/WHERE prontos. Detecta pela ausência de SELECT.
_DATE_COLUMN_PREFERRED = "DATA_REFERENCIA"
_DATE_COLUMN_EXCLUDE_PREFIXES = ("DT_PROCESSAMENTO", "DT_CADASTRO", "DT_ATUALIZACAO", "DT_CARGA", "DT_INSERCAO")
_DATE_COLUMN_TYPES = {"DATE", "TIMESTAMP", "DATETIME"}


def _is_bare_sql_expression(sql_template: str) -> bool:
    return bool(sql_template.strip()) and not re.search(r"\bselect\b", sql_template, re.IGNORECASE)


def _pick_date_column(columns: dict[str, str]) -> str | None:
    """Escolhe a coluna de data de referência de uma tabela, sem nenhuma \
    convenção hardcoded por gerência: prefere `DATA_REFERENCIA` (convenção \
    deste Gold Layer); senão, a primeira coluna de data que não pareça \
    timestamp de ETL/carga (`DT_PROCESSAMENTO` etc.)."""
    if _DATE_COLUMN_PREFERRED in columns:
        return _DATE_COLUMN_PREFERRED
    for name, col_type in columns.items():
        if col_type in _DATE_COLUMN_TYPES and not name.upper().startswith(_DATE_COLUMN_EXCLUDE_PREFIXES):
            return name
    return None


def _build_query_from_bare_expression(
    expression: str, source_table: str, project_id: str, date_start: str, date_end: str, limit: int
) -> tuple[str, str]:
    """Monta um SELECT executável a partir de uma expressão de agregação \
    + tabela fonte do Gold Metric Catalog. Detecta a coluna de data \
    dinamicamente via schema real do BigQuery — nada de nome de coluna/\
    tabela/dataset fixo, já que cada gerência tem sua própria tabela fato.

    Retorna (sql, date_column) — `date_column` vazio quando a tabela não tem \
    uma coluna de data identificável (a query sai sem filtro/quebra temporal).
    """
    columns = get_table_column_types(source_table, project_id)
    date_column = _pick_date_column(columns) or ""
    if date_column:
        sql = (
            f"SELECT {date_column} AS data_referencia, {expression} AS valor\n"
            f"FROM `{source_table}`\n"
            f"WHERE {date_column} BETWEEN '{date_start}' AND '{date_end}'\n"
            "GROUP BY 1\nORDER BY 1\n"
            f"LIMIT {limit}"
        )
    else:
        sql = f"SELECT {expression} AS valor\nFROM `{source_table}`\nLIMIT {limit}"
    return sql, date_column


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

    project_id = (context.get("project_id") or "").strip()
    if not project_id:
        return _err("project_id ausente para executar métrica.")

    raw_template = str(metric.get("sql_template") or "")
    if not raw_template.strip():
        return _err(f"Métrica '{key}' está sem sql_template.")

    if _is_bare_sql_expression(raw_template):
        # Gold Metric Catalog: expressão de agregação + SOURCE_TABLE — monta
        # o SELECT (em vez do render_sql de template completo abaixo).
        source_table = str(metric.get("source_table") or "").strip()
        if not source_table:
            return _err(f"Métrica '{key}' não tem SOURCE_TABLE configurado para montar a query.")
        default_start, default_end = semantic_layer.default_period()
        provided_params = args.get("params") or {}
        date_start = str(provided_params.get("date_start") or default_start)
        date_end = str(provided_params.get("date_end") or default_end)
        row_limit = int(provided_params.get("limit") or _DEFAULT_BQ_QUERY_MAX_ROWS)
        sql, _date_column = _build_query_from_bare_expression(
            raw_template.strip(), source_table, project_id, date_start, date_end, row_limit
        )
        params_used = {"date_start": date_start, "date_end": date_end, "limit": row_limit}
    else:
        sql, params_used = semantic_layer.render_sql(raw_template, args.get("params") or {})
        if not sql:
            return _err(f"Métrica '{key}' está sem sql_template.")
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
    CAPABILITY_CATALOG_SEARCH: cap_catalog_search,
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
    "cap_catalog_search",
    "cap_metric_lookup",
    "cap_metric_execute",
    "cap_org_fact_save",
    "cap_org_fact_recall",
    "cap_forecast_simple",
    "cap_attachment_analyze",
    "cap_chat_answer",
]
