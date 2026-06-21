"""Grafo LangGraph do Supervisor do Finance Voice IA.

Topologia:
    START
      → guardrails_in
      → persona_resolver        (altitude: coordenador/gerente/diretor)
      → response_mode_resolver  (estrutura: padrao/analise_profunda)
      → planner                 (LLM com structured output → PlanResponse)
      → router                  (executa os steps do plano em ondas — em
                                  paralelo quando independentes entre si)
      → composer                (LLM redige resposta final na altitude da
                                  persona e na estrutura do response_mode)
      → guardrails_out
      → END

Não há domínio fixo. O Planner escolhe entre capabilities genéricas (descoberta
de datasets, schema, text-to-sql, query livre, estatística, gráfico, chat).
"""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from src.agents.finance_auditor import audit as audit_log
from src.agents.finance_auditor import pii_guard
from src.agents.finance_auditor.capabilities import execute_capability
from src.agents.finance_auditor.personas import (
    PERSONA_GERAL,
    VALID_PERSONAS,
    detect_persona,
    get_persona_prompt,
)
from src.agents.finance_auditor.response_mode import (
    RESPONSE_MODE_ANALISE_PROFUNDA,
    RESPONSE_MODE_PADRAO,
    detect_response_mode,
    get_response_mode_prompt,
)
from src.agents.finance_auditor.supervisor_prompts import (
    COMPOSER_PROMPT_TEMPLATE,
    PLANNER_PROMPT,
    REFLECT_PROMPT,
)
from src.agents.finance_auditor.supervisor_schemas import (
    CAPABILITY_METRIC_EXECUTE,
    CAPABILITY_METRIC_LOOKUP,
    CAPABILITY_CHAT_ANSWER,
    PlanResponse,
    PlanStep,
    ReflectVerdict,
)
from src.agents.finance_auditor.supervisor_state import SupervisorState
from src.shared.tools.llm import invoke_with_retry

_MAX_PLAN_STEPS = 6
_MAX_ITERATIONS = 2  # router pode rodar até 2x (1 plano original + 1 retry pós-reflect)

# Capabilities consideradas "produtoras de resposta" — um plano sem nenhuma
# dessas é considerado incompleto pelo reflect.
_ANSWER_PRODUCING = {
    "text_to_sql",
    "bq_query",
    "metric_execute",
    "stats_describe",
    "forecast_simple",
    "attachment_analyze",
    "org_fact_recall",
    "chat_answer",
}

# Late binding: substitui ${PROJECT} e ${step_N.payload.path[.path...]} nos args.
_TPL_RE = re.compile(r"\$\{([^}]+)\}")


def _resolve_path(root: Any, path: str) -> Any:
    """Caminha um path tipo 'payload.datasets[0].table_id' sobre root."""
    parts: list[tuple[str, str]] = []  # (kind, value) — kind: 'key' | 'index'
    for m in re.finditer(r"([^.\[\]]+)|\[(\d+)\]", path):
        if m.group(1) is not None:
            parts.append(("key", m.group(1)))
        elif m.group(2) is not None:
            parts.append(("index", m.group(2)))

    cursor: Any = root
    for kind, value in parts:
        if cursor is None:
            return None
        if kind == "index":
            if not isinstance(cursor, list):
                return None
            try:
                cursor = cursor[int(value)]
            except IndexError:
                return None
        else:
            if not isinstance(cursor, dict):
                return None
            cursor = cursor.get(value)
    return cursor


def _resolve_placeholders(value: Any, prior_results: list[dict[str, Any]], project_id: str) -> Any:
    """Substitui ${PROJECT} e ${step_N.path} dentro de strings dos args."""
    if isinstance(value, str):
        def _sub(match: re.Match[str]) -> str:
            token = match.group(1).strip()
            if token == "PROJECT":
                return project_id or match.group(0)
            m = re.match(r"^step_(\d+)\.(.+)$", token)
            if not m:
                return match.group(0)
            idx = int(m.group(1))
            if idx >= len(prior_results):
                return match.group(0)
            entry = prior_results[idx] or {}
            if not entry.get("ok"):
                return match.group(0)
            resolved = _resolve_path(entry, m.group(2))
            return str(resolved) if resolved is not None else match.group(0)

        return _TPL_RE.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _resolve_placeholders(v, prior_results, project_id) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_placeholders(v, prior_results, project_id) for v in value]
    return value


_STEP_REF_RE = re.compile(r"^step_(\d+)\.")


def _step_dependencies(raw_args: Any) -> set[int]:
    """Índices de steps que este step referencia — via ${step_N.path} em
    qualquer string dos args (recursivo em dict/list) ou via o argumento
    direto `source_step_index` (usado por stats_describe/viz_spec/forecast).
    Usado pelo router para decidir quais steps podem rodar em paralelo."""
    deps: set[int] = set()

    def _scan(value: Any) -> None:
        if isinstance(value, str):
            for m in _TPL_RE.finditer(value):
                ref = _STEP_REF_RE.match(m.group(1).strip())
                if ref:
                    deps.add(int(ref.group(1)))
        elif isinstance(value, dict):
            for v in value.values():
                _scan(v)
        elif isinstance(value, list):
            for v in value:
                _scan(v)

    _scan(raw_args)

    if isinstance(raw_args, dict) and raw_args.get("source_step_index") is not None:
        try:
            deps.add(int(raw_args["source_step_index"]))
        except (TypeError, ValueError):
            pass

    return deps


_INJECTION_MARKERS = (
    "ignore previous",
    "ignore as instru",
    "desconsidere as instru",
    "system prompt",
    "</system>",
)


# ---------------------------------------------------------------------------
# Nó 1 — guardrails_in
# ---------------------------------------------------------------------------

def node_guardrails_in(state: SupervisorState) -> dict[str, Any]:
    text = (state.get("request_text") or "").lower()
    for marker in _INJECTION_MARKERS:
        if marker in text:
            return {
                "guardrail_in_ok": False,
                "guardrail_in_reason": "Detectada tentativa de prompt injection.",
                "warnings": ["Entrada bloqueada por guardrail de segurança."],
                "error": "Solicitação bloqueada por política de segurança.",
            }
    return {"guardrail_in_ok": True, "guardrail_in_reason": "", "warnings": []}


# ---------------------------------------------------------------------------
# Nó 2 — persona_resolver
# ---------------------------------------------------------------------------

def node_persona_resolver(state: SupervisorState) -> dict[str, Any]:
    persona = detect_persona(state.get("request_text", ""), state.get("user_profile") or {})
    if persona not in VALID_PERSONAS:
        persona = PERSONA_GERAL
    return {"persona": persona}


# ---------------------------------------------------------------------------
# Nó 2b — response_mode_resolver
# ---------------------------------------------------------------------------

def node_response_mode_resolver(state: SupervisorState) -> dict[str, Any]:
    """Decide a estrutura da resposta (independente da persona/altitude)."""
    mode = detect_response_mode(state.get("request_text", ""))
    return {"response_mode": mode}


# ---------------------------------------------------------------------------
# Nó 3 — planner
# ---------------------------------------------------------------------------

def _fallback_plan(reason: str) -> list[dict[str, Any]]:
    return [
        {
            "capability": CAPABILITY_CHAT_ANSWER,
            "args": {},
            "rationale": f"fallback do planner: {reason}",
        }
    ]


def _normalize_plan_steps(
    steps: list[dict[str, Any]],
    *,
    request_text: str,
) -> list[dict[str, Any]]:
    """Corrige pequenos desvios estruturais do planner antes da execucao."""
    normalized: list[dict[str, Any]] = []
    fallback_query = (request_text or "").strip()

    for step in steps:
        item = dict(step)
        capability = str(item.get("capability") or "").strip().lower()
        args = dict(item.get("args") or {})
        rationale = str(item.get("rationale") or "")

        if capability == CAPABILITY_METRIC_LOOKUP and not str(args.get("query") or "").strip():
            args["query"] = fallback_query
            item["args"] = args

        elif capability == CAPABILITY_METRIC_EXECUTE:
            key = str(args.get("key") or "").strip()
            if not key:
                query = (
                    str(args.get("query") or "").strip()
                    or str(args.get("name") or "").strip()
                    or str(args.get("metric") or "").strip()
                    or fallback_query
                )
                item = {
                    "capability": CAPABILITY_METRIC_LOOKUP,
                    "args": {"query": query},
                    "rationale": rationale or "normalizacao: localizar a metrica antes de executar",
                }

        normalized.append(item)
    return normalized


def node_planner(state: SupervisorState, llm: BaseChatModel) -> dict[str, Any]:
    if not state.get("guardrail_in_ok", True):
        return {"plan": [], "plan_rationale": "bloqueado por guardrail"}

    request_text = state.get("request_text", "")
    dataset_hint = str(state.get("dataset_hint") or "").strip()
    human_content = request_text
    if dataset_hint:
        human_content += (
            "\n\n"
            f"[CONTEXTO: o dataset já está definido como '{dataset_hint}' "
            "(gerência/área escolhida pelo usuário ou sessão). Use-o diretamente "
            "em dataset_ref/table_refs do step final — NÃO planeje bq_list_datasets "
            "para descobrir o dataset.]"
        )
    if state.get("response_mode") == RESPONSE_MODE_ANALISE_PROFUNDA:
        human_content += (
            "\n\n"
            "[CONTEXTO: o usuário pediu uma ANÁLISE PROFUNDA (causa raiz, "
            "impacto, plano de ação) — não apenas o número. Além do "
            "`text_to_sql` principal, planeje também `stats_describe` sobre "
            "os dados centrais e, se a pergunta envolver evolução no tempo, "
            "`forecast_simple`. Esses steps adicionais fundamentam as seções "
            "de causa raiz e impacto da resposta final — não entregue só os "
            "dados brutos.]"
        )
    try:
        structured_llm = llm.with_structured_output(PlanResponse)
        result: PlanResponse = invoke_with_retry(
            structured_llm,
            [
                SystemMessage(content=PLANNER_PROMPT),
                HumanMessage(content=human_content),
            ],
            max_attempts=2,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "plan": _fallback_plan(f"erro LLM: {exc}"),
            "plan_rationale": "fallback após erro do planner",
            "warnings": [*(state.get("warnings") or []), f"Planner falhou: {exc}"],
        }

    if not result or not result.steps:
        return {
            "plan": _fallback_plan("plano vazio"),
            "plan_rationale": "fallback: plano vazio",
        }

    steps = result.steps[:_MAX_PLAN_STEPS]
    normalized_steps = _normalize_plan_steps(
        [s.model_dump() if isinstance(s, PlanStep) else dict(s) for s in steps],
        request_text=request_text,
    )
    return {
        "plan": normalized_steps,
        "plan_rationale": result.rationale or "",
    }


# ---------------------------------------------------------------------------
# Nó 4 — router (executa steps em ondas: paralelo dentro da onda quando não
# há dependência entre eles via ${step_N...}/source_step_index; injeta LLM e
# tool_results)
# ---------------------------------------------------------------------------

def _run_step(
    idx: int,
    step: dict[str, Any],
    base_context: dict[str, Any],
    snapshot: list[dict[str, Any]],
) -> tuple[int, dict[str, Any], dict[str, Any]]:
    capability = str(step.get("capability") or "").strip().lower()
    raw_args = step.get("args") or {}

    # Late binding: ${PROJECT} e ${step_N.path} → valores reais.
    args = _resolve_placeholders(raw_args, snapshot, base_context.get("project_id", ""))

    # Encadeamento: cada step recebe os resultados anteriores no context.
    step_context = {**base_context, "tool_results": snapshot}

    outcome = execute_capability(capability, args, step_context)
    entry = {
        "step_index": idx,
        "capability": capability,
        "args": args,
        "ok": outcome.get("ok", False),
        "payload": outcome.get("payload"),
        "error": outcome.get("error"),
    }
    return idx, entry, outcome


def node_router(
    state: SupervisorState,
    llm: BaseChatModel,
    llm_creative: BaseChatModel,
) -> dict[str, Any]:
    plan = state.get("plan") or []
    base_context: dict[str, Any] = {
        "request_text": state.get("request_text", ""),
        "project_id": state.get("project_id", ""),
        "dataset_hint": state.get("dataset_hint"),
        "llm": llm,
        "llm_creative": llm_creative,
        "user": state.get("user") or {},
        "attachments": state.get("attachments") or [],
    }

    # Em re-execução (pós-reflect), preserva o que já foi executado.
    results: list[dict[str, Any] | None] = list(state.get("tool_results") or [])
    artifacts: list[dict[str, Any]] = list(state.get("artifacts") or [])
    warnings: list[str] = list(state.get("warnings") or [])
    start_idx = len(results)
    iteration = int(state.get("iteration") or 0) + 1

    new_steps = list(enumerate(plan[start_idx:], start=start_idx))
    results.extend([None] * len(new_steps))

    def _record(idx: int, entry: dict[str, Any], outcome: dict[str, Any]) -> None:
        results[idx] = entry
        for art in outcome.get("artifacts") or []:
            artifacts.append({"step_index": idx, **art})
        if not outcome.get("ok"):
            warnings.append(
                f"Step {idx} ({entry['capability']}) falhou: "
                f"{outcome.get('error') or 'erro desconhecido'}"
            )

    pending = new_steps
    while pending:
        # Snapshot fixo para esta onda: nenhum step "pronto" depende de outro
        # da MESMA onda (senão não estaria pronto), então todos podem ler o
        # mesmo retrato de tool_results sem se esperar.
        snapshot = list(results)

        ready: list[tuple[int, dict[str, Any]]] = []
        still_pending: list[tuple[int, dict[str, Any]]] = []
        for idx, step in pending:
            deps = _step_dependencies(step.get("args") or {})
            if all(0 <= d < idx and results[d] is not None for d in deps):
                ready.append((idx, step))
            else:
                still_pending.append((idx, step))

        if not ready:
            # Não devia acontecer com plans válidos (dependência circular ou
            # referência inválida) — executa o resto em ordem em vez de
            # travar silenciosamente num loop infinito.
            ready, still_pending = pending, []

        if len(ready) == 1:
            idx, step = ready[0]
            idx, entry, outcome = _run_step(idx, step, base_context, snapshot)
            _record(idx, entry, outcome)
        else:
            with ThreadPoolExecutor(max_workers=len(ready)) as pool:
                futures = [
                    pool.submit(_run_step, idx, step, base_context, snapshot)
                    for idx, step in ready
                ]
                for future in as_completed(futures):
                    idx, entry, outcome = future.result()
                    _record(idx, entry, outcome)

        pending = still_pending

    return {
        "tool_results": results,
        "artifacts": artifacts,
        "warnings": warnings,
        "iteration": iteration,
    }


# ---------------------------------------------------------------------------
# Nó 4b — reflect (auto-crítica + sugestão de retomada)
# ---------------------------------------------------------------------------

def _has_answer(tool_results: list[dict[str, Any]]) -> bool:
    """True se ao menos uma capability "produtora de resposta" teve sucesso."""
    return any(
        (r.get("capability") or "") in _ANSWER_PRODUCING and r.get("ok")
        for r in tool_results
    )


def node_reflect(state: SupervisorState, llm: BaseChatModel) -> dict[str, Any]:
    tool_results = state.get("tool_results") or []
    iteration = int(state.get("iteration") or 0)

    any_failed = any(not r.get("ok") for r in tool_results)
    has_answer = _has_answer(tool_results)
    needs_retry = any_failed or not has_answer

    # Sem motivo para refletir, ou já atingimos o teto de iterações.
    if not needs_retry or iteration >= _MAX_ITERATIONS:
        return {
            "reflect": {
                "is_valid": True,
                "confidence": 0.9 if has_answer and not any_failed else 0.5,
                "issues": (
                    []
                    if has_answer and not any_failed
                    else (
                        ["limite de iterações atingido"]
                        if iteration >= _MAX_ITERATIONS
                        else []
                    )
                ),
                "suggested_steps": [],
            }
        }

    summary = json.dumps(
        [
            {
                "step_index": r.get("step_index"),
                "capability": r.get("capability"),
                "ok": r.get("ok"),
                "error": r.get("error"),
            }
            for r in tool_results
        ],
        ensure_ascii=False,
    )
    user_content = (
        f"Pergunta original:\n{state.get('request_text', '')}\n\n"
        f"Execução até aqui (JSON):\n{summary}\n\n"
        f"Plano original: {json.dumps(state.get('plan') or [], ensure_ascii=False)}"
    )
    try:
        structured_llm = llm.with_structured_output(ReflectVerdict)
        verdict: ReflectVerdict = invoke_with_retry(
            structured_llm,
            [
                SystemMessage(content=REFLECT_PROMPT),
                HumanMessage(content=user_content),
            ],
            max_attempts=2,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "reflect": {
                "is_valid": True,
                "confidence": 0.0,
                "issues": [f"reflect falhou: {exc}"],
                "suggested_steps": [],
            }
        }

    if not verdict:
        return {"reflect": {"is_valid": True, "suggested_steps": []}}
    return {
        "reflect": {
            "is_valid": bool(verdict.is_valid),
            "confidence": float(verdict.confidence),
            "issues": list(verdict.issues),
            "suggested_steps": [s.model_dump() for s in verdict.suggested_steps],
        }
    }


def _reflect_router(state: SupervisorState) -> str:
    """Edge condicional: se reflect propôs steps, volta ao router; senão, composer."""
    reflect = state.get("reflect") or {}
    iteration = int(state.get("iteration") or 0)
    if reflect.get("is_valid", True):
        return "composer"
    suggested = reflect.get("suggested_steps") or []
    if not suggested or iteration >= _MAX_ITERATIONS:
        return "composer"
    return "router"


def _attach_retry_feedback(
    steps: list[dict[str, Any]], tool_results: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Propaga SQL + erro da última tentativa de text_to_sql que falhou para o
    retry sugerido pelo Reflect.

    Sem isso, `cap_text_to_sql` regeneraria a query do zero, sem saber o que
    já foi tentado nem por que o BigQuery rejeitou — a "autocorreção" do Reflect
    ficaria só no texto do plano, não na prática.
    """
    last_failed_sql_attempt: dict[str, Any] | None = None
    for r in tool_results:
        if r.get("capability") == "text_to_sql" and not r.get("ok"):
            last_failed_sql_attempt = r

    if not last_failed_sql_attempt:
        return steps

    payload = last_failed_sql_attempt.get("payload") or {}
    attempted_sql = str(payload.get("attempted_sql") or "").strip()
    error = str(last_failed_sql_attempt.get("error") or "").strip()
    if not attempted_sql or not error:
        return steps

    for step in steps:
        if step.get("capability") != "text_to_sql":
            continue
        step_args = dict(step.get("args") or {})
        step_args.setdefault("previous_sql", attempted_sql)
        step_args.setdefault("previous_error", error)
        step["args"] = step_args
    return steps


def node_apply_reflect_plan(state: SupervisorState) -> dict[str, Any]:
    """Anexa ao plano os steps sugeridos pelo reflect (limite global)."""
    reflect = state.get("reflect") or {}
    suggested = reflect.get("suggested_steps") or []
    if not suggested:
        return {}
    normalized = _normalize_plan_steps(
        [dict(step) for step in suggested],
        request_text=state.get("request_text", ""),
    )
    normalized = _attach_retry_feedback(normalized, state.get("tool_results") or [])
    plan = list(state.get("plan") or [])
    plan.extend(normalized)
    plan = plan[:_MAX_PLAN_STEPS * _MAX_ITERATIONS]
    return {"plan": plan}


# ---------------------------------------------------------------------------
# Nó 5 — composer (LLM redige resposta final adaptada à persona)
# ---------------------------------------------------------------------------

def _summarize_tool_results_for_llm(results: list[dict[str, Any]]) -> str:
    """Serializa os resultados truncando payloads grandes."""
    summary = []
    for r in results:
        payload = r.get("payload")
        if isinstance(payload, dict):
            payload_compact: dict[str, Any] = {}
            for k, v in payload.items():
                if isinstance(v, list) and len(v) > 20:
                    payload_compact[k] = v[:20]
                    payload_compact[f"{k}__truncated"] = True
                    payload_compact[f"{k}__total_count"] = len(v)
                elif isinstance(v, str) and len(v) > 4000:
                    payload_compact[k] = v[:4000] + "...[truncado]"
                else:
                    payload_compact[k] = v
            payload_view: Any = payload_compact
        else:
            payload_view = payload

        summary.append(
            {
                "step_index": r.get("step_index"),
                "capability": r.get("capability"),
                "ok": r.get("ok"),
                "error": r.get("error"),
                "payload": payload_view,
            }
        )
    try:
        return json.dumps(summary, ensure_ascii=False, indent=2, default=str)
    except Exception:  # noqa: BLE001
        return str(summary)


_TECH_LEAK_PATTERN = re.compile(
    r"\btimestamp\b|\bdatetime\b|\bbigquery\b|\bdry-?run\b|\bschema\b|"
    r"\btipo de dados?\b|\bincompat[ií]vel\b|"
    r"\bfun[cç][aã]o(?:\(?[oa]?es\)?)? que (?:eu )?utiliz\w*\b|"
    r"\bdetalhes? t[eé]cnicos?\b|\berro\s+t[eé]cnico\b",
    re.IGNORECASE,
)

_FAILURE_FALLBACK_ANSWER = (
    "## Resumo executivo\n\n"
    "Tentei responder à sua pergunta, mas não consegui concluir a análise "
    "com os dados disponíveis agora.\n\n"
    "Para tentar de novo com mais precisão, me diga o período exato que "
    "você quer analisar e se há algum recorte específico (produto, cliente "
    "ou categoria) que devo considerar."
)


def _looks_like_tech_leak(text: str) -> bool:
    """Detecta jargão técnico que o Composer foi instruído a nunca usar.

    Rede de segurança determinística (mesmo espírito do `pii_guard`): a
    instrução do prompt nem sempre é seguida à risca pelo LLM, então isso
    garante uma resposta profissional mesmo quando o modelo "vaza" detalhe
    de implementação ao explicar uma falha total.
    """
    return bool(_TECH_LEAK_PATTERN.search(text or ""))


def node_composer(state: SupervisorState, llm: BaseChatModel) -> dict[str, Any]:
    if state.get("error"):
        return {"final_answer": f"_{state['error']}_"}

    persona = state.get("persona") or PERSONA_GERAL
    persona_block = get_persona_prompt(persona)
    mode_block = get_response_mode_prompt(state.get("response_mode") or RESPONSE_MODE_PADRAO)
    system_prompt = COMPOSER_PROMPT_TEMPLATE.format(
        persona_block=persona_block, mode_block=mode_block
    )

    tool_results = state.get("tool_results") or []
    warnings = state.get("warnings") or []
    answer_succeeded = _has_answer(tool_results)

    user_content = (
        f"Pergunta original do usuário:\n{state.get('request_text', '')}\n\n"
        f"Resultados das capabilities (JSON):\n{_summarize_tool_results_for_llm(tool_results)}\n\n"
        f"Avisos (warnings): {json.dumps(warnings, ensure_ascii=False)}\n\n"
        f"Persona detectada: {persona}\n\n"
        "Redija a resposta final em Markdown."
    )

    try:
        response = invoke_with_retry(
            llm,
            [SystemMessage(content=system_prompt), HumanMessage(content=user_content)],
            max_attempts=2,
        )
        text = str(getattr(response, "content", response) or "").strip()
        if text:
            if not answer_succeeded and _looks_like_tech_leak(text):
                text = _FAILURE_FALLBACK_ANSWER
            return {"final_answer": text}
    except Exception as exc:  # noqa: BLE001
        return {
            "final_answer": _FAILURE_FALLBACK_ANSWER,
            "warnings": [*warnings, f"Composer falhou: {exc}"],
        }

    return {"final_answer": _FAILURE_FALLBACK_ANSWER}


# ---------------------------------------------------------------------------
# Nó 5b — audit (persiste auditoria antes do guard de saída)
# ---------------------------------------------------------------------------

def node_audit(state: SupervisorState) -> dict[str, Any]:
    audit_id = audit_log.record({
        "user_id": state.get("user_id") or "",
        "persona": state.get("persona") or "",
        "request_text": state.get("request_text") or "",
        "plan": state.get("plan") or [],
        "tool_results": state.get("tool_results") or [],
        "error": state.get("error") or "",
    })
    if audit_id is None:
        return {}
    return {"audit_id": audit_id}


# ---------------------------------------------------------------------------
# Nó 6 — guardrails_out (PII guard configurável)
# ---------------------------------------------------------------------------

def node_guardrails_out(state: SupervisorState) -> dict[str, Any]:
    result = pii_guard.apply_guard(
        final_answer=state.get("final_answer") or "",
        artifacts=state.get("artifacts") or [],
    )
    updates: dict[str, Any] = {
        "pii": {
            "mode": result.get("mode"),
            "pii_counts": result.get("pii_counts") or {},
            "blocked": bool(result.get("blocked")),
        }
    }
    if result.get("mode") != pii_guard.MODE_OFF:
        updates["final_answer"] = result.get("final_answer") or ""
        updates["artifacts"] = result.get("artifacts") or []
    pii_counts = result.get("pii_counts") or {}
    if pii_counts:
        warnings = list(state.get("warnings") or [])
        warnings.append(
            "PII detectada e tratada (modo "
            f"{result.get('mode')}): {', '.join(sorted(pii_counts))}"
        )
        updates["warnings"] = warnings
    return updates


# ---------------------------------------------------------------------------
# build_graph
# ---------------------------------------------------------------------------

def build_supervisor_graph(
    llm: BaseChatModel,
    llm_creative: BaseChatModel | None = None,
) -> Any:
    """Compila o grafo Supervisor.

    Args:
        llm: LLM analítico (baixa temperatura) — Planner e text_to_sql.
        llm_creative: LLM criativo — Composer. Cai para `llm` quando omitido.
    """
    _composer_llm = llm_creative or llm
    workflow = StateGraph(SupervisorState)

    workflow.add_node("guardrails_in", node_guardrails_in)
    workflow.add_node("persona_resolver", node_persona_resolver)
    workflow.add_node("response_mode_resolver", node_response_mode_resolver)
    workflow.add_node("planner", lambda s: node_planner(s, llm=llm))
    workflow.add_node(
        "router",
        lambda s: node_router(s, llm=llm, llm_creative=_composer_llm),
    )
    workflow.add_node("reflect", lambda s: node_reflect(s, llm=llm))
    workflow.add_node("apply_reflect_plan", node_apply_reflect_plan)
    workflow.add_node("composer", lambda s: node_composer(s, llm=_composer_llm))
    workflow.add_node("audit", node_audit)
    workflow.add_node("guardrails_out", node_guardrails_out)

    workflow.add_edge(START, "guardrails_in")
    workflow.add_edge("guardrails_in", "persona_resolver")
    workflow.add_edge("persona_resolver", "response_mode_resolver")
    workflow.add_edge("response_mode_resolver", "planner")
    workflow.add_edge("planner", "router")
    workflow.add_edge("router", "reflect")
    workflow.add_conditional_edges(
        "reflect",
        _reflect_router,
        {"router": "apply_reflect_plan", "composer": "composer"},
    )
    workflow.add_edge("apply_reflect_plan", "router")
    workflow.add_edge("composer", "audit")
    workflow.add_edge("audit", "guardrails_out")
    workflow.add_edge("guardrails_out", END)

    return workflow.compile()
