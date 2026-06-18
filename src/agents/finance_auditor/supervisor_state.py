"""Estado compartilhado do grafo Supervisor do Finance Voice IA."""

from __future__ import annotations

from typing import Any, TypedDict


class SupervisorState(TypedDict, total=False):
    """Estado do Supervisor + Specialists."""

    # --- Entrada ---
    request_text: str
    project_id: str
    dataset_hint: str | None
    user_profile: dict[str, Any]  # vindo da sessão de chat (profile)
    user_id: str                  # username/id do dono da sessão (RBAC + audit)
    user: dict[str, Any]          # sessão completa (passada às capabilities)

    # --- Guardrails ---
    guardrail_in_ok: bool
    guardrail_in_reason: str

    # --- Persona ---
    persona: str  # coordenador | gerente | diretor | geral

    # --- Response Mode ---
    response_mode: str  # padrao | analise_profunda

    # --- Planner ---
    plan: list[dict[str, Any]]      # lista de steps: {capability, args, rationale}
    plan_rationale: str

    # --- Execução (router) ---
    tool_results: list[dict[str, Any]]  # [{step_index, capability, ok, payload, error}]

    # --- Composição final ---
    final_answer: str           # texto narrativo (markdown)
    artifacts: list[dict[str, Any]]  # tabelas, SQL, refs — para o frontend renderizar

    # --- Fase 3 (governance) ---
    pii: dict[str, Any]         # {mode, pii_counts, blocked}
    audit_id: int               # id da entrada gravada em finance_audit_log

    # --- Fase 4 ---
    attachments: list[dict[str, Any]]   # anexos enviados (CSV/imagem)
    iteration: int                       # nº de execuções do router (1-based)
    reflect: dict[str, Any]              # último verdict do nó reflect

    # --- Controle ---
    warnings: list[str]
    error: str | None
