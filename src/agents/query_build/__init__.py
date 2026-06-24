from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from typing import Any

from langgraph.types import Command

from src.agents.query_build.graph import build_graph
from src.agents.query_build.state import QueryBuildState
from src.core.base_agent import BaseAgent
from src.shared.tools.llm import create_llm

_LOG = logging.getLogger(__name__)

# Mesmo registro de tipos customizados que o query_analyzer já precisa —
# QueryBuildState também carrega um Pydantic customizado (DryRunResult), que
# o MemorySaver não precisaria serializar, mas o registro é defensivo e
# barato (evita warning de desserialização do msgpack do LangGraph).
_MSGPACK_MODULES = "src.shared.tools.schemas,src.agents.query_build.state"
_existing = os.environ.get("LANGGRAPH_ALLOWED_MSGPACK_MODULES", "")
if _MSGPACK_MODULES not in _existing:
    os.environ["LANGGRAPH_ALLOWED_MSGPACK_MODULES"] = ",".join(filter(None, [_existing, _MSGPACK_MODULES]))


def _make_checkpointer():
    """MemorySaver, não SqliteSaver — mesmo motivo do query_analyzer: o
    estado carrega um DryRunResult (Pydantic customizado) que o SqliteSaver
    não consegue desserializar via msgpack sem registro extra. HITL
    funciona normalmente enquanto o processo do servidor está de pé.
    """
    from langgraph.checkpoint.memory import MemorySaver
    return MemorySaver()


# Singleton — sobrevive entre chamadas analyze()/resume() do mesmo processo.
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
        _LOG.info("Cleaned %d expired Query Build thread(s) from registry", len(expired))


def _start_background_cleanup() -> None:
    def _loop() -> None:
        while True:
            time.sleep(300)
            try:
                with _THREAD_REGISTRY_LOCK:
                    _cleanup_expired_threads()
            except Exception as exc:
                _LOG.warning("Background cleanup do Query Build falhou: %s", exc)

    t = threading.Thread(target=_loop, name="qb-thread-registry-cleanup", daemon=True)
    t.start()


_start_background_cleanup()


class QueryBuildAgent(BaseAgent):
	def __init__(self) -> None:
		self._graph = None

	@property
	def agent_id(self) -> str:
		return "query_build"

	@property
	def display_name(self) -> str:
		return "Query Build"

	def _get_graph(self):
		if self._graph is None:
			self._graph = build_graph(create_llm(), _CHECKPOINTER)
		return self._graph

	def analyze(
		self,
		query: str,
		project_id: str,
		dataset_hint: str | None = None,
		user: dict[str, Any] | None = None,
		thread_id: str | None = None,
	) -> dict[str, Any]:
		graph = self._get_graph()
		tid = thread_id or str(uuid.uuid4())
		config = {"configurable": {"thread_id": tid}}

		initial_state = QueryBuildState(
			request_text=query,
			project_id=project_id,
			dataset_hint=dataset_hint,
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
			raise RuntimeError("Nao foi possivel gerar SQL para a solicitacao.")

		return self._format_result(final_event)

	def resume(self, thread_id: str, human_decision: str) -> dict[str, Any]:
		with _THREAD_REGISTRY_LOCK:
			thread_known = thread_id in _THREAD_REGISTRY

		if not thread_known:
			_LOG.warning(
				"resume() chamado para thread %s ausente do registry (TTL expirado ou servidor reiniciou).",
				thread_id,
			)
			raise RuntimeError(
				"Sessão de geração de SQL expirou ou foi perdida (servidor reiniciado). "
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
				"Não foi possível retomar a geração de SQL. Inicie uma nova solicitação."
			) from exc

		snapshot = graph.get_state(config)
		if snapshot.next:
			return self._interrupted_response(thread_id, final_event)

		if not final_event:
			raise RuntimeError("Geração de SQL não produziu resultado após retomada.")

		return self._format_result(final_event)

	# ── helpers ──────────────────────────────────────────────────────────────

	def _interrupted_response(
		self,
		thread_id: str,
		last_event: dict[str, Any] | None,
	) -> dict[str, Any]:
		event = last_event or {}
		return {
			"status": "awaiting_approval",
			"thread_id": thread_id,
			"generated_sql": event.get("generated_sql"),
			"quality_score": event.get("quality_score"),
			"quality_issues": event.get("quality_issues") or [],
		}

	def _format_result(self, final_event: dict[str, Any]) -> dict[str, Any]:
		dry = final_event.get("dry_run_generated")
		warnings = final_event.get("warnings") or []
		has_error = bool(final_event.get("error") or (dry and dry.error))

		return {
			"request_text": final_event.get("request_text"),
			"generated_sql": final_event.get("generated_sql"),
			"explanation": final_event.get("explanation") or "",
			"assumptions": final_event.get("assumptions") or [],
			"warnings": warnings,
			"quality_score": final_event.get("quality_score"),
			"quality_issues": final_event.get("quality_issues") or [],
			"dry_run": {
				"bytes_processed": dry.bytes_processed if dry else None,
				"estimated_cost_usd": dry.estimated_cost_usd if dry else None,
				"error": dry.error if dry else None,
			},
			"sample_data": {
				"columns": final_event.get("sample_columns") or [],
				"rows": final_event.get("sample_rows") or [],
				"error": final_event.get("sample_error"),
			},
			"status": "ok" if final_event.get("generated_sql") and not has_error else "error",
			"error": final_event.get("error"),
		}

	def runtime_info(self) -> dict[str, str]:
		return {
			"provider": "shared",
			"provider_label": "Mesmo provider do runtime",
			"model": "Mesmo modelo configurado no .env",
		}


__all__ = ["QueryBuildAgent"]
