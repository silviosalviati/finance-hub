"""Estado compartilhado do grafo Supervisor do Finance Voice IA."""

from __future__ import annotations

from typing import Any, TypedDict


class SupervisorState(TypedDict, total=False):
    """Estado do Supervisor + Specialists."""

    # --- Entrada ---
    request_text: str
    project_id: str
    dataset_hint: str | None
    conversation_context: str  # últimos turnos da sessão (query+resposta), p/ follow-ups
    last_analysis_markdown: str  # última análise concluída na sessão (base do podcast)
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

    # --- Observabilidade de custo ---
    # Lista mutável compartilhada (não é reducer-merged) — cada chamada LLM
    # via invoke_with_retry(usage_sink=...) faz append de {label, model,
    # input_tokens, output_tokens, total_tokens}. Criada em
    # FinanceAuditorAgent.analyze() e passada por referência via initial_state;
    # como nenhum node "retorna" essa chave, o objeto original persiste
    # mutado ao longo de toda a execução do grafo, sem precisar de reducer.
    usage_log: list[dict[str, Any]]

    # Cache mutável compartilhado, escopado à requisição (não persiste entre
    # requisições, não é reducer-merged — mesmo princípio de `usage_log`).
    # Evita refazer schema/catalog_search/pick_relevant_tables do zero
    # quando duas capabilities pedem a mesma coisa no mesmo turno, ou quando
    # um retry do Reflect executa `text_to_sql` de novo para os mesmos
    # table_refs. Chaves namespaced por prefixo: "schema:{table_ref}",
    # "catalog_search:{project_id}:{query}:{top_k}",
    # "pick_tables:{dataset_project}.{dataset_id}:{natural_language}".
    context_cache: dict[str, Any]

    # --- Fase 4 ---
    attachments: list[dict[str, Any]]   # anexos enviados (CSV/imagem)
    iteration: int                       # nº de execuções do router (1-based)
    reflect: dict[str, Any]              # último verdict do nó reflect
    podcast_requested: bool               # pedido explícito de podcast/áudio

    # --- Controle ---
    warnings: list[str]
    error: str | None
