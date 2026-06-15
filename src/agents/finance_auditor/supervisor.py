"""Grafo LangGraph do Supervisor do Finance Voice IA (fase 1).

Topologia:

    START
      → guardrails_in
      → persona_resolver
      → planner          (LLM com structured output → PlanResponse)
      → router           (executa cada step do plano em sequência)
      → composer         (LLM redige resposta final na altitude da persona)
      → guardrails_out
      → END

Diferenças vs. grafo legado (FinanceAuditor):
- Não assume domínio único (VoC); o pipeline VoC vira UMA capability.
- Decide dinamicamente quais capabilities chamar.
- Adapta a resposta à persona detectada (Coordenador / Gerente / Diretor).
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

# Limite duro para evitar planos absurdos vindos do LLM.
_MAX_PLAN_STEPS = 5

# Padrões de injeção / instrução suspeita (guardrail simples — fase 1).
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
        # short-circuit: composer dará a mensagem de bloqueio
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
# Nó 4 — router (executa cada step em sequência)
# ---------------------------------------------------------------------------

def node_router(state: SupervisorState, legacy_agent: Any) -> dict[str, Any]:
    plan = state.get("plan") or []
    context = {
        "request_text": state.get("request_text", ""),
        "project_id": state.get("project_id", ""),
        "dataset_hint": state.get("dataset_hint"),
        "legacy_agent": legacy_agent,
    }

    results: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    warnings: list[str] = list(state.get("warnings") or [])

    for idx, step in enumerate(plan):
        capability = str(step.get("capability") or "").strip().lower()
        args = step.get("args") or {}
        outcome = execute_capability(capability, args, context)

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
    """Serializa os resultados para o LLM sem estourar tokens.

    Trunca payloads grandes (ex.: muitas linhas) preservando o essencial.
    """
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


def _shortcut_voc_markdown(state: SupervisorState) -> str | None:
    """Atalho: se o plano foi só voc_report bem-sucedido, devolve o markdown gerado.

    Evita uma chamada LLM redundante para reescrever um relatório já completo.
    """
    plan = state.get("plan") or []
    results = state.get("tool_results") or []
    if len(plan) != 1 or len(results) != 1:
        return None
    only = results[0]
    if only.get("capability") != "voc_report" or not only.get("ok"):
        return None
    payload = only.get("payload") or {}
    md = str(payload.get("markdown_report") or "").strip()
    return md or None


def node_composer(state: SupervisorState, llm: BaseChatModel) -> dict[str, Any]:
    if state.get("error"):
        # bloqueado por guardrail anterior
        return {"final_answer": f"_{state['error']}_"}

    shortcut = _shortcut_voc_markdown(state)
    if shortcut:
        return {"final_answer": shortcut}

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
        text = getattr(response, "content", None) or str(response)
        text = str(text).strip()
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
# Nó 6 — guardrails_out (placeholder fase 1)
# ---------------------------------------------------------------------------

def node_guardrails_out(state: SupervisorState) -> dict[str, Any]:
    # Fase 1: passthrough. Hooks de PII / política de saída entram na fase 3.
    return {}


# ---------------------------------------------------------------------------
# build_graph
# ---------------------------------------------------------------------------

def build_supervisor_graph(
    llm: BaseChatModel,
    llm_creative: BaseChatModel | None,
    legacy_agent: Any,
) -> Any:
    """Compila o grafo Supervisor.

    Args:
        llm: LLM analítico (baixa temperatura) — Planner.
        llm_creative: LLM criativo (maior temperatura) — Composer. Cai para `llm` quando omitido.
        legacy_agent: instância de FinanceAuditorAgent (para a capability voc_report).
    """
    _composer_llm = llm_creative or llm
    workflow = StateGraph(SupervisorState)

    workflow.add_node("guardrails_in", node_guardrails_in)
    workflow.add_node("persona_resolver", node_persona_resolver)
    workflow.add_node("planner", lambda s: node_planner(s, llm=llm))
    workflow.add_node("router", lambda s: node_router(s, legacy_agent=legacy_agent))
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
