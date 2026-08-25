from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from typing import Any

from langgraph.types import Command

from src.agents.query_transformer.graph import build_graph
from src.agents.query_transformer.state import QueryTransformerState
from src.core.base_agent import BaseAgent
from src.shared.tools.llm import create_llm

_LOG = logging.getLogger(__name__)

_FRIENDLY_ERRORS: dict[str, str] = {
    "rbac": (
        "Você não tem permissão para acessar uma das tabelas referenciadas. "
        "Peça liberação de acesso ou revise a SQL."
    ),
    "sql_not_select": (
        "Só é possível converter consultas de leitura (SELECT). "
        "Modelos Dataform não executam comandos de escrita."
    ),
    "security_policy": (
        "Essa entrada contém uma operação ou estrutura que não pode ser convertida com segurança. "
        "Revise a SQL e, se necessário, separe as etapas manualmente."
    ),
    "sql_parse": (
        "Não foi possível interpretar essa SQL no dialeto BigQuery. "
        "Revise a sintaxe antes de converter."
    ),
    "architecture_confirmation": (
        "A estratégia da transformação precisa ser confirmada antes de gerar o SQLX."
    ),
    "bigquery_syntax": (
        "A SQL não passou na validação técnica do BigQuery. "
        "Verifique a sintaxe e os nomes de tabela/coluna."
    ),
    "llm_api": (
        "Não foi possível gerar o SQLX agora. Tente novamente em instantes."
    ),
}
_FRIENDLY_ERROR_DEFAULT = (
    "Não foi possível concluir a conversão. Tente novamente ou revise a SQL enviada."
)


def _friendlify_error(raw_error: str | None, category: str) -> str:
    if not raw_error:
        return _FRIENDLY_ERROR_DEFAULT
    return _FRIENDLY_ERRORS.get(category, _FRIENDLY_ERROR_DEFAULT)


# Mesmo registro defensivo de tipos customizados que query_build/query_analyzer
# precisam (o estado carrega DryRunResult, um Pydantic customizado).
_MSGPACK_MODULES = "src.shared.tools.schemas,src.agents.query_transformer.state"
_existing = os.environ.get("LANGGRAPH_ALLOWED_MSGPACK_MODULES", "")
if _MSGPACK_MODULES not in _existing:
    os.environ["LANGGRAPH_ALLOWED_MSGPACK_MODULES"] = ",".join(filter(None, [_existing, _MSGPACK_MODULES]))


def _make_checkpointer():
    """MemorySaver, não SqliteSaver — mesmo motivo do query_build: o estado
    carrega um DryRunResult que o SqliteSaver não desserializa via msgpack
    sem registro extra. HITL funciona enquanto o processo do servidor está
    de pé; estado se perde num restart (mesmo trade-off aceito nos outros
    2 agentes com HITL).
    """
    from langgraph.checkpoint.memory import MemorySaver
    return MemorySaver()


_CHECKPOINTER = _make_checkpointer()

_THREAD_REGISTRY: dict[str, float] = {}
_THREAD_REGISTRY_LOCK = threading.Lock()
_THREAD_TTL = 3600  # 1 hora


def _register_thread(thread_id: str) -> None:
    with _THREAD_REGISTRY_LOCK:
        _THREAD_REGISTRY[thread_id] = time.time()
        _cleanup_expired_threads()


def _cleanup_expired_threads() -> None:
    now = time.time()
    expired = [tid for tid, ts in _THREAD_REGISTRY.items() if now - ts > _THREAD_TTL]
    for tid in expired:
        _THREAD_REGISTRY.pop(tid, None)
        try:
            storage = _CHECKPOINTER.storage
            keys_to_delete = [k for k in storage if isinstance(k, tuple) and k[0] == tid]
            for k in keys_to_delete:
                del storage[k]
        except Exception as exc:
            _LOG.debug("Failed to evict checkpoint storage for thread %s: %s", tid, exc)
    if expired:
        _LOG.info("Cleaned %d expired Query Transformer thread(s) from registry", len(expired))


def _start_background_cleanup() -> None:
    def _loop() -> None:
        while True:
            time.sleep(300)
            try:
                with _THREAD_REGISTRY_LOCK:
                    _cleanup_expired_threads()
            except Exception as exc:
                _LOG.warning("Background cleanup do Query Transformer falhou: %s", exc)

    t = threading.Thread(target=_loop, name="qt-thread-registry-cleanup", daemon=True)
    t.start()


_start_background_cleanup()


class QueryTransformerAgent(BaseAgent):
    def __init__(self) -> None:
        self._graph = None

    @property
    def agent_id(self) -> str:
        return "query_transformer"

    @property
    def display_name(self) -> str:
        return "Query Transformer"

    def _get_graph(self):
        if self._graph is None:
            self._graph = build_graph(create_llm(), _CHECKPOINTER)
        return self._graph

    def analyze(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None = None,  # não usado — SQL de entrada já é totalmente qualificada
        user: dict[str, Any] | None = None,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        graph = self._get_graph()
        tid = thread_id or str(uuid.uuid4())
        config = {"configurable": {"thread_id": tid}}

        initial_state = QueryTransformerState(
            request_sql=query,
            project_id=project_id,
            user=user or {},
        )

        _register_thread(tid)

        final_event: dict[str, Any] | None = None
        for event in graph.stream(initial_state, config=config, stream_mode="values"):
            final_event = event

        snapshot = graph.get_state(config)
        if snapshot.next:
            return self._interrupted_response(tid, final_event)

        if not final_event:
            raise RuntimeError("Não foi possível converter a SQL para SQLX.")

        return self._format_result(final_event)

    def resume(self, thread_id: str, human_decision: str | dict[str, Any]) -> dict[str, Any]:
        with _THREAD_REGISTRY_LOCK:
            thread_known = thread_id in _THREAD_REGISTRY

        if not thread_known:
            _LOG.warning(
                "resume() chamado para thread %s ausente do registry (TTL expirado ou servidor reiniciou).",
                thread_id,
            )
            raise RuntimeError(
                "Sessão de conversão expirou ou foi perdida (servidor reiniciado). "
                "Inicie uma nova solicitação."
            )

        graph = self._get_graph()
        config = {"configurable": {"thread_id": thread_id}}

        final_event: dict[str, Any] | None = None
        try:
            for event in graph.stream(
                Command(resume=human_decision),
                config=config,
                stream_mode="values",
            ):
                final_event = event
        except Exception as exc:
            _LOG.warning("Falha ao retomar thread %s: %s", thread_id, exc)
            raise RuntimeError(
                "Não foi possível retomar a conversão. Inicie uma nova solicitação."
            ) from exc

        snapshot = graph.get_state(config)
        if snapshot.next:
            return self._interrupted_response(thread_id, final_event)

        if not final_event:
            raise RuntimeError("Conversão não produziu resultado após retomada.")

        return self._format_result(final_event)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _interrupted_response(
        self,
        thread_id: str,
        last_event: dict[str, Any] | None,
    ) -> dict[str, Any]:
        event = last_event or {}
        return {
            "status": "awaiting_requirements" if not event.get("requirements_confirmed") and event.get("required_questions") else "awaiting_approval",
            "thread_id": thread_id,
            "sqlx_content": event.get("sqlx_content"),
            "quality_score": event.get("quality_score"),
            "quality_issues": event.get("quality_issues") or [],
            "equivalence_ok": event.get("equivalence_ok"),
            "equivalence_diff": event.get("equivalence_diff"),
            "recommendation": event.get("architecture_recommendation"),
            "confidence": event.get("architecture_confidence"),
            "questions": event.get("required_questions") or [],
            "user_answers": event.get("user_answers") or {},
        }

    def _format_result(self, final_event: dict[str, Any]) -> dict[str, Any]:
        dry = final_event.get("dry_run_generated")
        raw_error = final_event.get("error")
        has_error = bool(raw_error or (dry and dry.error))
        error_detail = raw_error or (dry.error if dry else "")

        report_lines = [
            f"**Tipo de materialização escolhido:** `{final_event.get('materialization_type') or '—'}`",
            "",
            final_event.get("rationale") or "",
        ]
        refs = final_event.get("suggested_refs") or []
        if refs:
            report_lines += ["", "**Refs sugeridos:** " + ", ".join(f"`{r}`" for r in refs)]
        if final_event.get("equivalence_ok"):
            report_lines += ["", "✅ Validação de equivalência: schema e custo compatíveis com a SQL original."]
        elif final_event.get("equivalence_diff"):
            report_lines += ["", f"⚠️ Validação de equivalência: {final_event.get('equivalence_diff')}"]

        return {
            "sqlx_content": final_event.get("sqlx_content") or "",
            "markdown_report": "\n".join(line for line in report_lines if line is not None),
            "materialization_type": final_event.get("materialization_type") or "",
            "quality_score": final_event.get("quality_score"),
            "quality_issues": final_event.get("quality_issues") or [],
            "equivalence_ok": final_event.get("equivalence_ok"),
            "equivalence_diff": final_event.get("equivalence_diff") or "",
            "warnings": final_event.get("warnings") or [],
            "cost_reduction_pct": final_event.get("cost_reduction_pct", 0.0),
            "dry_run": {
                "bytes_processed": dry.bytes_processed if dry else None,
                "estimated_cost_usd": dry.estimated_cost_usd if dry else None,
                "error": dry.error if dry else None,
            },
            "artifacts": (
                [{"type": "code", "language": "sqlx", "content": final_event.get("sqlx_content") or ""}]
                if final_event.get("sqlx_content")
                else []
            ),
            "status": "ok" if final_event.get("sqlx_content") and not has_error else "error",
            "error": (
                (
                    f"{_friendlify_error(error_detail, final_event.get('error_category') or '')} "
                    f"Detalhe: {error_detail}"
                )
                if error_detail
                else None
            ),
        }

    def runtime_info(self) -> dict[str, str]:
        return {
            "provider": "shared",
            "provider_label": "Mesmo provider do runtime",
            "model": "Mesmo modelo configurado no .env",
        }


__all__ = ["QueryTransformerAgent"]
