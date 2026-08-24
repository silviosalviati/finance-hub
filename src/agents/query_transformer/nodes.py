from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.types import interrupt
from pydantic import BaseModel, Field

from src.agents.query_transformer.knowledge_base import retrieve_best_practices
from src.agents.query_transformer.prompts import (
    QUERY_TRANSFORMER_SYSTEM_PROMPT,
    _QUALITY_JUDGE_SYSTEM_PROMPT,
)
from src.agents.query_transformer.state import QueryTransformerState
from src.shared.config import get_runtime_config
from src.shared.guardrails import rbac
from src.shared.guardrails.audit import record as record_audit_entry
from src.shared.guardrails.sql_safety import assert_select_only
from src.shared.tools.bigquery import dry_run_query
from src.shared.tools.llm import invoke_with_retry
from src.shared.tools.schemas import DryRunResult

TABLE_REF_PATTERN = r"`?([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)`?"
REF_PLACEHOLDER_PATTERN = r"\$\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}"
SOURCE_PLACEHOLDER_PATTERN = (
    r"\$\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}"
)
SELECT_STAR_PATTERN = r"select\s+\*"


class _SqlxOutput(BaseModel):
    """Saída estruturada de `generate_sqlx` — evita parsing frágil de texto
    livre/markdown, mesmo padrão de `_SqlOutput`/`_Picked` no Finance Auditor.
    """

    config_block: str = Field(description="Bloco config { ... } do Dataform")
    query_body: str = Field(description="Corpo da query SQL, sem o config()")
    materialization_type: str = Field(description='"table" | "view" | "incremental"')
    suggested_refs: list[str] = Field(default_factory=list)
    rationale: str = ""


def check_access(state: QueryTransformerState) -> dict[str, Any]:
    """RBAC sobre os datasets/tabelas referenciados na SQL de entrada — antes
    de qualquer chamada de LLM, mesmo motivo do `check_access` do Query Build.
    """
    tables = sorted(set(re.findall(TABLE_REF_PATTERN, state.request_sql)))
    for table_ref in tables:
        parts = table_ref.split(".")
        dataset_hint = parts[1] if len(parts) == 3 else table_ref
        allowed, reason = rbac.check_dataset(state.user, dataset_hint)
        if not allowed:
            return {
                "error": f"Você não tem permissão para acessar '{table_ref}'.",
                "error_category": "rbac",
                "warnings": [f"Bloqueado por RBAC: {reason}"] if reason else [],
            }
    return {}


def node_guardrails_in(state: QueryTransformerState) -> dict[str, Any]:
    """Bloqueia SQL vazia, não-SELECT (DDL/DML) ou sem FROM — modelos
    Dataform são transformações de leitura, não faz sentido converter um
    comando de escrita.
    """
    safety_error = assert_select_only(state.request_sql)
    if safety_error:
        return {
            "error": f"Apenas SQL de leitura pode virar modelo Dataform: {safety_error}",
            "error_category": "sql_not_select",
        }

    if not re.search(r"\bfrom\b", state.request_sql, flags=re.IGNORECASE):
        return {
            "error": "A SQL não referencia nenhuma tabela (sem cláusula FROM) — não há o que converter.",
            "error_category": "sql_not_select",
        }

    return {}


def dry_run_original(state: QueryTransformerState) -> dict[str, Any]:
    if state.error:
        return {}

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(dry_run_query, state.request_sql, state.project_id)
            result = future.result(timeout=20)
    except TimeoutError:
        return {
            "error": "BigQuery dry-run da SQL original excedeu o timeout de 20 segundos.",
            "error_category": "bigquery_syntax",
        }
    except Exception as exc:
        result = DryRunResult(
            error=f"Erro ao executar dry-run: {exc}",
            bytes_processed=0,
            bytes_billed=0,
            estimated_cost_usd=0.0,
        )

    if result.error:
        return {
            "dry_run_original": result,
            "error": f"A SQL original não passou na validação técnica do BigQuery (dry-run): {result.error}",
            "error_category": "bigquery_syntax",
        }

    return {"dry_run_original": result}


def _extract_message_content(response: Any) -> str:
    if hasattr(response, "content"):
        return str(response.content).strip()
    return str(response).strip()


def generate_sqlx(state: QueryTransformerState, llm: BaseChatModel) -> dict[str, Any]:
    # Reentrada após erro recuperável (dry_run_generated falhou tecnicamente)
    # com repair_attempts < 1 — ver _guard_repairable em graph.py. Não há
    # early-return em `state.error` aqui: este nó É o alvo do repair.
    is_repair = bool(state.error)
    repair_attempts = state.repair_attempts + 1 if is_repair else state.repair_attempts

    # Reentrada por decisão humana "melhorar" após score baixo ou falha de
    # equivalência (await_quality_approval) — state.error já está limpo
    # nesse ponto, só _guard/_guard_repairable roteiam com error setado.
    is_quality_retry = not is_repair and state.human_decision == "melhorar"
    quality_retry_count = (
        state.quality_retry_count + 1 if is_quality_retry else state.quality_retry_count
    )

    feedback_block = ""
    if is_repair:
        feedback_block = (
            f"\nTENTATIVA ANTERIOR FALHOU: {state.error}\n"
            "Corrija especificamente esse problema na nova versão.\n"
        )
    elif is_quality_retry and state.equivalence_diff:
        feedback_block = (
            f"\nA VERSÃO ANTERIOR FALHOU NA VALIDAÇÃO DE EQUIVALÊNCIA: "
            f"{state.equivalence_diff}. Corrija o corpo da query para produzir "
            "exatamente o mesmo resultado da SQL original.\n"
        )
    elif is_quality_retry:
        issues_text = "; ".join(state.quality_issues) or "qualidade insuficiente"
        feedback_block = (
            f"\nA VERSÃO ANTERIOR TEVE NOTA {state.quality_score}/100. "
            f"Problemas identificados: {issues_text}. Gere uma nova versão que "
            "corrija esses pontos, preservando a mesma intenção de negócio.\n"
        )

    rag_snippets = retrieve_best_practices(state.request_sql)
    system_prompt = QUERY_TRANSFORMER_SYSTEM_PROMPT.replace(
        "__RAG_BLOCK__", "\n".join(f"- {s}" for s in rag_snippets)
    )

    user_prompt = f"""
SQL do BigQuery a converter:
{state.request_sql}

Project ID: {state.project_id}
{feedback_block}
"""

    try:
        structured_llm = llm.with_structured_output(_SqlxOutput)
        result: _SqlxOutput = invoke_with_retry(
            structured_llm,
            [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)],
            max_attempts=2,
            label="query_transformer_generate",
        )

        if not result.query_body.strip():
            return {
                "error": "A LLM não retornou um corpo de query válido.",
                "error_category": "llm_api",
                "repairable_error": False,
                "repair_attempts": repair_attempts,
                "quality_retry_count": quality_retry_count,
            }

        config_block = result.config_block.strip()
        query_body = result.query_body.strip()
        sqlx_content = f"{config_block}\n\n{query_body}\n" if config_block else f"{query_body}\n"

        return {
            "config_block": config_block,
            "query_body": query_body,
            "materialization_type": result.materialization_type.strip() or "table",
            "suggested_refs": result.suggested_refs,
            "rationale": result.rationale,
            "sqlx_content": sqlx_content,
            "error": None,
            "error_category": "",
            "repairable_error": False,
            "repair_attempts": repair_attempts,
            "quality_retry_count": quality_retry_count,
        }
    except Exception as exc:
        return {
            "error": f"Falha ao gerar SQLX: {exc}",
            "error_category": "llm_api",
            "repairable_error": False,
            "repair_attempts": repair_attempts,
            "quality_retry_count": quality_retry_count,
        }


def _resolve_refs_to_literal_sql(query_body: str, project_id: str) -> str:
    """Substitui `${ref("x")}`/`${source("d","t")}` de volta pelo nome de
    tabela literal, só para poder rodar um dry-run — não há compilador
    Dataform real disponível (decisão de escopo do design).
    """
    resolved = re.sub(
        REF_PLACEHOLDER_PATTERN,
        lambda m: f"`{project_id}.{m.group(1)}`",
        query_body,
    )
    resolved = re.sub(
        SOURCE_PLACEHOLDER_PATTERN,
        lambda m: f"`{project_id}.{m.group(1)}.{m.group(2)}`",
        resolved,
    )
    return resolved


def dry_run_generated(state: QueryTransformerState) -> dict[str, Any]:
    if state.error or not state.query_body:
        return {}

    literal_sql = _resolve_refs_to_literal_sql(state.query_body, state.project_id)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(dry_run_query, literal_sql, state.project_id)
            result = future.result(timeout=20)
    except TimeoutError:
        return {
            "error": "BigQuery dry-run do SQLX gerado excedeu o timeout de 20 segundos.",
            "error_category": "bigquery_syntax",
            "repairable_error": True,
        }
    except Exception as exc:
        result = DryRunResult(
            error=f"Erro ao executar dry-run: {exc}",
            bytes_processed=0,
            bytes_billed=0,
            estimated_cost_usd=0.0,
        )

    if result.error:
        return {
            "dry_run_generated": result,
            "warnings": [*state.warnings, f"Dry-run do SQLX gerado retornou erro: {result.error}"],
            "error": "O SQLX gerado não passou na validação técnica do BigQuery (dry-run).",
            "error_category": "bigquery_syntax",
            "repairable_error": True,
        }

    return {"dry_run_generated": result}


def validate_equivalence(state: QueryTransformerState) -> dict[str, Any]:
    """Comparação determinística (sem LLM) entre o dry-run da SQL original e
    o do SQLX gerado — schema de colunas do resultado + custo/bytes. Não
    executa nenhuma query de verdade (decisão de escopo do design).
    """
    if state.error:
        return {}

    original = state.dry_run_original
    generated = state.dry_run_generated
    if not original or not generated:
        return {"equivalence_ok": False, "equivalence_diff": "Dry-run indisponível para comparação."}

    original_cols = [(c["name"].lower(), c["type"]) for c in original.result_schema]
    generated_cols = [(c["name"].lower(), c["type"]) for c in generated.result_schema]

    if original_cols == generated_cols:
        return {"equivalence_ok": True, "equivalence_diff": ""}

    diff = (
        f"Schema original: {original_cols} != schema gerado: {generated_cols}"
        if original_cols != generated_cols
        else ""
    )
    return {"equivalence_ok": False, "equivalence_diff": diff}


def _deterministic_quality_checks(state: QueryTransformerState) -> tuple[int, list[str]]:
    score = 100
    issues: list[str] = []

    if not state.config_block or "config" not in state.config_block.lower():
        score -= 30
        issues.append("O SQLX não tem um bloco config() válido.")

    if re.search(SELECT_STAR_PATTERN, state.query_body, flags=re.IGNORECASE):
        score -= 15
        issues.append("Usa SELECT * em vez de listar colunas explicitamente.")

    has_ref = bool(re.search(REF_PLACEHOLDER_PATTERN, state.query_body)) or bool(
        re.search(SOURCE_PLACEHOLDER_PATTERN, state.query_body)
    )
    has_raw_table = bool(re.search(TABLE_REF_PATTERN, state.query_body))
    if has_raw_table and not has_ref:
        score -= 25
        issues.append("Referencia tabela por nome totalmente qualificado em vez de ref()/source().")

    if state.materialization_type == "incremental" and "uniquekey" not in state.config_block.lower():
        score -= 10
        issues.append("Materialização incremental sem uniqueKey declarada no config().")

    return max(0, score), issues


def score_quality(state: QueryTransformerState, llm: BaseChatModel) -> dict[str, Any]:
    if state.error or not state.query_body:
        return {}

    score, issues = _deterministic_quality_checks(state)

    judge_prompt = f"""
SQL original:
{state.request_sql}

SQLX gerado (config):
{state.config_block}

SQLX gerado (query):
{state.query_body}

Tipo de materialização escolhido: {state.materialization_type}
"""

    try:
        response = invoke_with_retry(
            llm,
            [SystemMessage(content=_QUALITY_JUDGE_SYSTEM_PROMPT), HumanMessage(content=judge_prompt)],
            max_attempts=2,
            label="query_transformer_score",
        )
        raw = _extract_message_content(response)
        cleaned = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        import json

        judged = json.loads(cleaned)

        if judged.get("same_intent_ok") is False:
            score -= 30
            issues.append(f"Possível mudança de intenção: {judged.get('same_intent_reason', '')}")
        if judged.get("materialization_sensible_ok") is False:
            score -= 10
            issues.append(
                f"Materialização pode ser inadequada: {judged.get('materialization_sensible_reason', '')}"
            )
    except Exception as exc:
        issues.append(f"Avaliação por LLM indisponível; nota baseada só em checagens automáticas ({exc}).")

    return {"quality_score": max(0, min(100, score)), "quality_issues": issues}


def await_quality_approval(state: QueryTransformerState) -> dict[str, Any]:
    """HITL nativo do LangGraph — pausa se o score ficar baixo OU a
    validação de equivalência falhar (equivalência falha sempre pausa,
    independente do score: correção do resultado nunca é negociável).
    """
    min_score = int(get_runtime_config("QUERY_TRANSFORMER_MIN_QUALITY_SCORE", "80"))
    equivalence_failed = not state.equivalence_ok

    if state.quality_score >= min_score and not equivalence_failed:
        return {"human_decision": "skip"}

    if state.quality_retry_count >= 2 and not equivalence_failed:
        warnings = list(state.warnings)
        warnings.append(
            f"Não foi possível elevar a nota acima de {min_score} após 2 tentativas; "
            f"seguindo com a melhor versão obtida (nota atual: {state.quality_score})."
        )
        return {"human_decision": "skip", "warnings": warnings}

    decision = interrupt({
        "message": (
            "A conversão não passou na validação de equivalência de resultado."
            if equivalence_failed
            else f"O SQLX gerado tem nota {state.quality_score}/100. Deseja seguir assim ou melhorar?"
        ),
        "score": state.quality_score,
        "issues": state.quality_issues,
        "equivalence_ok": state.equivalence_ok,
        "equivalence_diff": state.equivalence_diff,
        "sqlx_content": state.sqlx_content,
    })
    return {"human_decision": decision}


def node_guardrails_out(state: QueryTransformerState) -> dict[str, Any]:
    # Sem PII em SQL/SQLX (é definição de transformação, não dado de linha) —
    # nada a mascarar aqui, diferente do Query Build (que devolve sample_rows).
    return {}


def record_audit(state: QueryTransformerState) -> dict[str, Any]:
    """Fan-in final — roda sempre, sucesso ou erro. Reaproveita a mesma
    `finance_audit_log` genérica que o Query Build também usa (representa a
    si mesmo como um plano de 1 step)."""
    dry = state.dry_run_generated
    tool_results = [{
        "ok": bool(state.sqlx_content) and not state.error,
        "payload": {
            "bytes_processed": dry.bytes_processed if dry and not dry.error else 0,
            "estimated_cost_usd": dry.estimated_cost_usd if dry and not dry.error else 0,
        },
    }]
    plan = [{
        "capability": "query_transformer_generate_sqlx",
        "materialization_type": state.materialization_type,
        "quality_score": state.quality_score,
        "quality_issues": state.quality_issues,
        "equivalence_ok": state.equivalence_ok,
        "error_category": state.error_category,
    }]
    record_audit_entry({
        "user_id": str((state.user or {}).get("username") or ""),
        "request_text": state.request_sql,
        "plan": plan,
        "tool_results": tool_results,
        "error": state.error or "",
    })
    return {}
