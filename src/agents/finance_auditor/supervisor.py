"""Grafo LangGraph do Supervisor do Finance Voice IA.

Topologia:
    START
      → guardrails_in
      → persona_resolver
      → planner            (LLM com structured output → PlanResponse)
      → router             (executa cada step do plano em sequência)
      → composer           (LLM redige resposta final na altitude da persona)
      → guardrails_out
      → END

Não há domínio fixo. O Planner escolhe entre capabilities genéricas (descoberta
de datasets, schema, text-to-sql, query livre, estatística, gráfico, chat).
"""

from __future__ import annotations

import json
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from src.agents.finance_auditor.capabilities import execute_capability
from src.agents.finance_auditor.personas import (
    PERSONA_GERAL,
    VALID_PERSONAS,
    detect_persona,
    get_persona_prompt,
)
from src.agents.finance_auditor.supervisor_prompts import (
    COMPOSER_PROMPT_TEMPLATE,
    PLANNER_PROMPT,
)
from src.agents.finance_auditor.supervisor_schemas import (
    CAPABILITY_CHAT_ANSWER,
    PlanResponse,
    PlanStep,
)
from src.agents.finance_auditor.supervisor_state import SupervisorState
from src.shared.tools.llm import invoke_with_retry

_MAX_PLAN_STEPS = 6

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


def node_planner(state: SupervisorState, llm: BaseChatModel) -> dict[str, Any]:
    if not state.get("guardrail_in_ok", True):
        return {"plan": [], "plan_rationale": "bloqueado por guardrail"}

    request_text = state.get("request_text", "")
    try:
        structured_llm = llm.with_structured_output(PlanResponse)
        result: PlanResponse = invoke_with_retry(
            structured_llm,
            [
                SystemMessage(content=PLANNER_PROMPT),
                HumanMessage(content=request_text),
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
    return {
        "plan": [s.model_dump() if isinstance(s, PlanStep) else dict(s) for s in steps],
        "plan_rationale": result.rationale or "",
    }


# ---------------------------------------------------------------------------
# Nó 4 — router (executa cada step em sequência; injeta LLM e tool_results)
# ---------------------------------------------------------------------------

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
    }

    results: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    warnings: list[str] = list(state.get("warnings") or [])

    for idx, step in enumerate(plan):
        capability = str(step.get("capability") or "").strip().lower()
        args = step.get("args") or {}

        # Encadeamento: cada step recebe os resultados anteriores no context.
        step_context = {**base_context, "tool_results": list(results)}

        outcome = execute_capability(capability, args, step_context)

        results.append(
            {
                "step_index": idx,
                "capability": capability,
                "args": args,
                "ok": outcome.get("ok", False),
                "payload": outcome.get("payload"),
                "error": outcome.get("error"),
            }
        )
        for art in outcome.get("artifacts") or []:
            artifacts.append({"step_index": idx, **art})

        if not outcome.get("ok"):
            warnings.append(
                f"Step {idx} ({capability}) falhou: {outcome.get('error') or 'erro desconhecido'}"
            )

    return {"tool_results": results, "artifacts": artifacts, "warnings": warnings}


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


def node_composer(state: SupervisorState, llm: BaseChatModel) -> dict[str, Any]:
    if state.get("error"):
        return {"final_answer": f"_{state['error']}_"}

    persona = state.get("persona") or PERSONA_GERAL
    persona_block = get_persona_prompt(persona)
    system_prompt = COMPOSER_PROMPT_TEMPLATE.format(persona_block=persona_block)

    tool_results = state.get("tool_results") or []
    warnings = state.get("warnings") or []

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
            return {"final_answer": text}
    except Exception as exc:  # noqa: BLE001
        return {
            "final_answer": (
                "Não foi possível gerar a resposta final neste momento. "
                f"Detalhes técnicos: {exc}"
            ),
            "warnings": [*warnings, f"Composer falhou: {exc}"],
        }

    return {"final_answer": "Sem conteúdo gerado pelo Composer."}


# ---------------------------------------------------------------------------
# Nó 6 — guardrails_out (placeholder — política de PII entra em fase posterior)
# ---------------------------------------------------------------------------

def node_guardrails_out(state: SupervisorState) -> dict[str, Any]:
    return {}


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
    workflow.add_node("planner", lambda s: node_planner(s, llm=llm))
    workflow.add_node(
        "router",
        lambda s: node_router(s, llm=llm, llm_creative=_composer_llm),
    )
    workflow.add_node("composer", lambda s: node_composer(s, llm=_composer_llm))
    workflow.add_node("guardrails_out", node_guardrails_out)

    workflow.add_edge(START, "guardrails_in")
    workflow.add_edge("guardrails_in", "persona_resolver")
    workflow.add_edge("persona_resolver", "planner")
    workflow.add_edge("planner", "router")
    workflow.add_edge("router", "composer")
    workflow.add_edge("composer", "guardrails_out")
    workflow.add_edge("guardrails_out", END)

    return workflow.compile()
