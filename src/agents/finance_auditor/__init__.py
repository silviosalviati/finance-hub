"""Finance Voice IA — assistente analítico genérico sobre BigQuery.

Único modo de execução: Supervisor + Specialists.

    guardrails_in → persona_resolver → planner → router → composer → guardrails_out

Sem domínio fixo. As capabilities são descobertas e encadeadas dinamicamente
pelo Planner conforme a pergunta do usuário.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any

from langchain_core.callbacks.usage import get_usage_metadata_callback
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command

from src.agents.finance_auditor.supervisor import (
    PODCAST_CONFIRM_MESSAGE,
    PODCAST_VOICE_OPTIONS,
    build_supervisor_graph,
)
from src.agents.finance_auditor.personas import VALID_PERSONAS
from src.core.base_agent import BaseAgent
from src.shared.config import get_runtime_config
from src.shared.tools.llm import create_llm as _create_llm
from src.shared.tools.llm import summarize_usage_by_label


def _make_checkpointer() -> SqliteSaver:
    """Checkpointer nativo do LangGraph, persistido em disco.

    `SqliteSaver`, não `MemorySaver` (diferente de query_build/query_analyzer)
    — `SupervisorState` é 100% JSON-safe (sem objetos Pydantic customizados
    como os irmãos carregam), então dá pra ter persistência real: se o
    processo cair no meio de um plano de vários passos, o snapshot de estado
    sobrevive ao restart (`MemorySaver` se perderia junto com o processo,
    resolvendo o problema só pela metade). Thread-safe via lock interno do
    `SqliteSaver` — seguro sob `asyncio.to_thread` com múltiplas requisições
    concorrentes.
    """
    db_path = Path(".sixth") / "finance_auditor_checkpoints.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    checkpointer = SqliteSaver(conn)
    checkpointer.setup()
    return checkpointer


# Singleton — sobrevive entre chamadas analyze() do mesmo processo, mesmo
# padrão de query_build/query_analyzer.
_CHECKPOINTER = _make_checkpointer()


class FinanceAuditorAgent(BaseAgent):
    """Agente conversacional de análise de dados."""

    def __init__(self) -> None:
        self._graph = None

    @property
    def agent_id(self) -> str:
        return "finance_auditor"

    @property
    def display_name(self) -> str:
        return "Finance Voice IA"

    def _get_graph(self):
        if self._graph is None:
            llm = _create_llm()
            t_creative = float(get_runtime_config("VERTEXAI_TEMPERATURE_CREATIVE", "0.3"))
            llm_creative = _create_llm(temperature=t_creative)
            # Tiering de modelo (2.11): FINANCE_AUDITOR_LITE_MODEL vazio (default)
            # = usa o mesmo `llm` de sempre, zero mudança de comportamento.
            # Só ativa um modelo mais barato para tarefas simples (pick_relevant_tables,
            # veredito do Reflect) quando configurado explicitamente no painel admin.
            lite_model = get_runtime_config("FINANCE_AUDITOR_LITE_MODEL", "").strip()
            llm_lite = _create_llm(model=lite_model) if lite_model else llm
            self._graph = build_supervisor_graph(
                llm=llm, llm_creative=llm_creative, llm_lite=llm_lite, checkpointer=_CHECKPOINTER
            )
        return self._graph

    def analyze(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None = None,
        conversation_context: str = "",
        last_analysis_markdown: str = "",
        user_profile: dict[str, Any] | None = None,
        user: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        """Executa o grafo Supervisor e devolve um dict compatível com o frontend.

        `thread_id` identifica a execução para o checkpointer nativo do
        LangGraph — gerado automaticamente quando omitido (não há resume()
        exposto ainda; o checkpointer por ora só garante que o snapshot de
        estado sobrevive a um crash do processo no meio de um plano)."""
        graph = self._get_graph()
        tid = thread_id or str(uuid.uuid4())
        config = {"configurable": {"thread_id": tid}}

        u = user or {}
        # Lista mutável passada por referência — cada invoke_with_retry(...,
        # usage_sink=...) dentro do grafo faz append nela (ver
        # SupervisorState.usage_log). Populada ao fim da execução, permite
        # reconstruir consumo de tokens por nó (planner/reflect/composer/
        # capabilities), não só o agregado por modelo do callback abaixo.
        usage_log: list[dict[str, Any]] = []
        # Cache turn-scoped de schema/catalog_search/pick_relevant_tables —
        # ver docstring de SupervisorState.context_cache.
        context_cache: dict[str, Any] = {}
        initial_state = {
            "request_text": query,
            "thread_id": tid,
            "project_id": project_id,
            "dataset_hint": dataset_hint,
            "conversation_context": conversation_context,
            "last_analysis_markdown": last_analysis_markdown,
            "user_profile": user_profile or {},
            "user_id": str(u.get("username") or u.get("user_id") or ""),
            "user": u,
            "attachments": list(attachments or []),
            "usage_log": usage_log,
            "context_cache": context_cache,
        }

        final_state: dict[str, Any] | None = None
        # Captura o uso real de tokens de TODA chamada LLM feita durante a
        # execução (planner, reflect, composer e qualquer LLM interno de
        # capability, ex.: text_to_sql) — funciona mesmo com structured
        # output, pois opera no nível de callback do provider, não no
        # objeto já parseado.
        with get_usage_metadata_callback() as usage_cb:
            for event in graph.stream(initial_state, config=config, stream_mode="values"):
                final_state = event
            token_usage = self._summarize_token_usage(usage_cb.usage_metadata)
            token_usage["by_node"] = summarize_usage_by_label(usage_log)

        # `podcast_builder` pode pausar o grafo via interrupt() aguardando
        # confirmação humana antes de gastar a chamada de TTS — `snapshot.next`
        # não-vazio indica que o grafo parou no meio (composer/audit já
        # rodaram, então final_state já traz a análise completa do turno).
        snapshot = graph.get_state(config)
        if snapshot.next:
            response = self._build_response(final_state, tid, token_usage)
            response["status"] = "awaiting_approval"
            response["message"] = PODCAST_CONFIRM_MESSAGE
            response["voice_options"] = list(PODCAST_VOICE_OPTIONS)
            response["tone_options"] = list(VALID_PERSONAS)
            return response

        return self._build_response(final_state, tid, token_usage)

    def resume(
        self,
        thread_id: str,
        human_decision: str,
        voice_gender: str | None = None,
        tone: str | None = None,
    ) -> dict[str, Any]:
        """Retoma o grafo pausado em `node_podcast_builder` após a decisão
        humana sobre gerar (ou não) o podcast — ver `interrupt()` no nó.
        `voice_gender`/`tone` só importam quando `human_decision == "approve"`.
        """
        graph = self._get_graph()
        config = {"configurable": {"thread_id": thread_id}}

        snapshot = graph.get_state(config)
        if not snapshot.values:
            raise RuntimeError(
                "Sessão de análise expirou ou não foi encontrada. Faça a pergunta novamente."
            )

        resume_payload = {
            "decision": human_decision,
            "voice_gender": voice_gender,
            "tone": tone,
        }
        final_state: dict[str, Any] | None = None
        with get_usage_metadata_callback() as usage_cb:
            for event in graph.stream(
                Command(resume=resume_payload), config=config, stream_mode="values"
            ):
                final_state = event
            token_usage = self._summarize_token_usage(usage_cb.usage_metadata)
            token_usage["by_node"] = summarize_usage_by_label(
                (final_state or {}).get("usage_log") or []
            )

        return self._build_response(final_state, thread_id, token_usage)

    @staticmethod
    def _build_response(
        final_state: dict[str, Any] | None, tid: str, token_usage: dict[str, Any]
    ) -> dict[str, Any]:
        if not final_state:
            return {
                "status": "error",
                "error": "Supervisor não produziu resultado.",
                "markdown_report": "",
                "warnings": [],
                "token_usage": token_usage,
                "thread_id": tid,
            }

        if final_state.get("error"):
            return {
                "status": "error",
                "error": final_state["error"],
                "markdown_report": final_state.get("final_answer", ""),
                "warnings": final_state.get("warnings", []),
                "token_usage": token_usage,
                "thread_id": tid,
            }

        return {
            "status": "ok",
            "response_mode": "analysis",
            # Estrutura que o Composer de fato usou ("padrao" = Resumo
            # executivo + achados; "analise_profunda" = diagnóstico em 5
            # seções, SEM Resumo executivo por desenho — ver response_mode.py).
            # Não confundir com "response_mode" acima, que é o roteamento da
            # API (chat/analysis); o frontend usa este campo pra saber se
            # deve completar um "## Resumo executivo" que faltou.
            "composer_mode": final_state.get("response_mode") or "padrao",
            "persona": final_state.get("persona", ""),
            "plan": final_state.get("plan", []),
            "plan_rationale": final_state.get("plan_rationale", ""),
            "tool_results": final_state.get("tool_results", []),
            "artifacts": final_state.get("artifacts", []),
            # Campos compatíveis com o frontend atual:
            "markdown_report": final_state.get("final_answer", ""),
            "chat_answer": final_state.get("final_answer", ""),
            "warnings": final_state.get("warnings", []),
            "pii": final_state.get("pii", {}),
            "audit_id": final_state.get("audit_id"),
            "token_usage": token_usage,
            "thread_id": tid,
        }

    @staticmethod
    def _summarize_token_usage(usage_by_model: dict[str, dict[str, int]]) -> dict[str, Any]:
        total_tokens = 0
        input_tokens = 0
        output_tokens = 0
        cache_read_tokens = 0
        for usage in (usage_by_model or {}).values():
            total_tokens += int(usage.get("total_tokens") or 0)
            input_tokens += int(usage.get("input_tokens") or 0)
            output_tokens += int(usage.get("output_tokens") or 0)
            details = usage.get("input_token_details") or {}
            cache_read_tokens += int(details.get("cache_read") or 0)
        return {
            "total_tokens": total_tokens,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            # Tokens do input servidos do cache do Vertex (implícito ou
            # explícito) em vez de reprocessados do zero — 0 enquanto não
            # confirmado que o cache está ativo (ver finance-voice.md, 2.7).
            "cache_read_tokens": cache_read_tokens,
            "by_model": usage_by_model or {},
        }

    def runtime_info(self) -> dict[str, str]:
        return {
            "agent_id": self.agent_id,
            "display_name": self.display_name,
            "supervisor_nodes": (
                "guardrails_in,persona_resolver,response_mode_resolver,planner,"
                "router,composer,podcast_builder,audit,guardrails_out"
            ),
            "capabilities": (
                "bq_list_datasets,bq_list_tables,bq_get_schema,bq_query,"
                "text_to_sql,stats_describe,viz_spec,"
                "metric_lookup,metric_execute,chat_answer"
            ),
        }
