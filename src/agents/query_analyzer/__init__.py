from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from typing import Any

from langgraph.types import Command

_LOG = logging.getLogger(__name__)

# Registrar tipos customizados para evitar warnings de desserialização do msgpack do LangGraph.
# Sem isso, DryRunResult/QueryAntiPattern/IntelligenceReport geram warnings ao retomar checkpoints.
_MSGPACK_MODULES = "src.shared.tools.schemas,src.agents.query_analyzer.state"
_existing = os.environ.get("LANGGRAPH_ALLOWED_MSGPACK_MODULES", "")
if _MSGPACK_MODULES not in _existing:
    os.environ["LANGGRAPH_ALLOWED_MSGPACK_MODULES"] = ",".join(filter(None, [_existing, _MSGPACK_MODULES]))

from src.agents.query_analyzer.graph import build_graph
from src.agents.query_analyzer.state import AgentState
from src.core.base_agent import BaseAgent
from src.shared.config import get_runtime_config
from src.shared.tools.llm import create_llm as _create_llm


def _make_checkpointer():
    """Usa MemorySaver para evitar falha de desserialização de tipos Pydantic customizados.

    SqliteSaver serializa o estado via msgpack e não consegue desserializar
    DryRunResult/QueryAntiPattern/OptimizationReport sem registro explícito.
    MemorySaver mantém tudo em memória — HITL funciona normalmente enquanto o processo está ativo.
    """
    from langgraph.checkpoint.memory import MemorySaver
    return MemorySaver()


# Singleton checkpointer — sobrevive a corridas de inicialização e mantém
# thread_ids de HITL entre chamadas analyze/resume.
_CHECKPOINTER = _make_checkpointer()

# Thread creation timestamps for TTL cleanup
_THREAD_REGISTRY: dict[str, float] = {}
_THREAD_REGISTRY_LOCK = threading.Lock()
_THREAD_TTL = 3600  # 1 hour


def _register_thread(thread_id: str) -> None:
    with _THREAD_REGISTRY_LOCK:
        _THREAD_REGISTRY[thread_id] = time.time()
        _cleanup_expired_threads()


def _cleanup_expired_threads() -> None:
    """Remove threads expirados do MemorySaver (chamado com lock já adquirido)."""
    now = time.time()
    expired = [tid for tid, ts in _THREAD_REGISTRY.items() if now - ts > _THREAD_TTL]
    for tid in expired:
        _THREAD_REGISTRY.pop(tid, None)
        # Clear from MemorySaver internal storage
        try:
            storage = _CHECKPOINTER.storage
            keys_to_delete = [k for k in storage if isinstance(k, tuple) and k[0] == tid]
            for k in keys_to_delete:
                del storage[k]
        except Exception as exc:
            _LOG.debug("Failed to evict checkpoint storage for thread %s: %s", tid, exc)
    if expired:
        _LOG.info("Cleaned %d expired HITL thread(s) from registry", len(expired))


def _start_background_cleanup() -> None:
    """Daemon que roda cleanup a cada 5 min — protege contra registries que crescem sem novas chamadas analyze()."""
    def _loop() -> None:
        while True:
            time.sleep(300)
            try:
                with _THREAD_REGISTRY_LOCK:
                    _cleanup_expired_threads()
            except Exception as exc:
                _LOG.warning("Background cleanup falhou: %s", exc)

    t = threading.Thread(target=_loop, name="qa-thread-registry-cleanup", daemon=True)
    t.start()


_start_background_cleanup()


class QueryAnalyzerAgent(BaseAgent):
    def __init__(self) -> None:
        self._graph = None

    @property
    def agent_id(self) -> str:
        return "query_analyzer"

    @property
    def display_name(self) -> str:
        return "Query Analyzer"

    def _get_graph(self):
        if self._graph is None:
            llm = _create_llm()
            t_creative = float(get_runtime_config("VERTEXAI_TEMPERATURE_CREATIVE", "0.3"))
            llm_creative = _create_llm(temperature=t_creative)
            self._graph = build_graph(llm, _CHECKPOINTER, llm_creative=llm_creative)
        return self._graph

    def analyze(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None = None,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        """Executa o pipeline de análise.

        Retorna `{"status": "ok", ...}` quando concluído ou
        `{"status": "awaiting_approval", "thread_id": ..., ...}` quando pausado para aprovação.
        Use `resume(thread_id, decision)` para continuar.
        """
        graph = self._get_graph()
        tid = thread_id or str(uuid.uuid4())
        config = {"configurable": {"thread_id": tid}}

        max_iters = int(get_runtime_config("QA_MAX_ITERATIONS", "2"))

        initial_state = AgentState(
            original_query=query,
            project_id=project_id,
            dataset_hint=dataset_hint,
            max_iterations=max_iters,
        )

        _register_thread(tid)

        final_event: dict[str, Any] | None = None
        for event in graph.stream(initial_state, config=config, stream_mode="values"):
            final_event = event

        snapshot = graph.get_state(config)
        if snapshot.next:
            return self._interrupted_response(tid, final_event)

        if not final_event or not final_event.get("report"):
            raise RuntimeError("Análise não produziu relatório.")

        return self._format_result(final_event)

    def resume(self, thread_id: str, human_decision: str) -> dict[str, Any]:
        """Retoma o pipeline após decisão humana.

        Args:
            thread_id: Identificador retornado pelo `analyze()` em 'awaiting_approval'.
            human_decision: 'approve' para otimizar, 'skip' para ir direto ao relatório.
        """
        with _THREAD_REGISTRY_LOCK:
            thread_known = thread_id in _THREAD_REGISTRY

        if not thread_known:
            _LOG.warning(
                "resume() chamado para thread %s ausente do registry (TTL expirado ou servidor reiniciou).",
                thread_id,
            )
            raise RuntimeError(
                "Sessão de análise expirou ou foi perdida (servidor reiniciado). "
                "Clique em 'Reанalisar' para iniciar uma nova análise."
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
                "Não foi possível retomar a análise. "
                "Por favor, inicie uma nova análise clicando em 'Reанalisar'."
            ) from exc

        if not final_event or not final_event.get("report"):
            raise RuntimeError("Análise não produziu relatório após retomada.")

        return self._format_result(final_event)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _interrupted_response(
        self,
        thread_id: str,
        last_event: dict[str, Any] | None,
    ) -> dict[str, Any]:
        antipatterns_raw = (last_event or {}).get("antipatterns") or []
        antipatterns_data = [
            ap.model_dump() if hasattr(ap, "model_dump") else ap
            for ap in antipatterns_raw
        ]
        dry = (last_event or {}).get("dry_run_original")
        return {
            "status": "awaiting_approval",
            "thread_id": thread_id,
            "needs_optimization": bool((last_event or {}).get("needs_optimization")),
            "antipatterns": antipatterns_data,
            "bytes_processed": dry.bytes_processed if dry and not dry.error else None,
            "estimated_cost_usd": dry.estimated_cost_usd if dry and not dry.error else None,
        }

    def _format_result(self, final_event: dict[str, Any]) -> dict[str, Any]:
        report = final_event["report"]
        dry_orig = final_event.get("dry_run_original")
        dry_opt = final_event.get("dry_run_optimized")

        return {
            "status": "ok",
            "efficiency_score": report.efficiency_score,
            "grade": report.grade,
            "summary": report.summary,
            "antipatterns": [
                {
                    "pattern": ap.pattern,
                    "description": ap.description,
                    "severity": ap.severity,
                    "suggestion": ap.suggestion,
                }
                for ap in report.antipatterns_found
            ],
            "optimized_query": report.optimized_query,
            "original_query": report.original_query,
            "intelligence_summary": report.intelligence_summary,
            "bytes_original": dry_orig.bytes_processed if dry_orig and not dry_orig.error else None,
            "bytes_optimized": dry_opt.bytes_processed if dry_opt and not dry_opt.error else None,
            "cost_original_usd": dry_orig.estimated_cost_usd if dry_orig and not dry_orig.error else None,
            "cost_optimized_usd": dry_opt.estimated_cost_usd if dry_opt and not dry_opt.error else None,
            "bytes_saved": report.bytes_saved,
            "cost_saved_usd": report.cost_saved_usd,
            "savings_pct": report.savings_pct,
            "recommendations": report.recommendations,
            "power_bi_tips": report.power_bi_tips,
            "applied_optimizations": report.applied_optimizations,
            "dry_run_error": dry_orig.error if dry_orig else None,
            "data_existence_warning": report.data_existence_warning,
            "optimization_status": report.optimization_status,
            "data_quality": report.data_quality,
        }

    def runtime_info(self) -> dict[str, str]:
        provider = get_runtime_config("LLM_PROVIDER", "vertexai").lower()

        if provider == "vertexai":
            return {
                "provider": "vertexai",
                "provider_label": "Vertex AI",
                "model": get_runtime_config("VERTEXAI_MODEL", "gemini-2.5-flash"),
            }

        return {
            "provider": provider,
            "provider_label": "Provider desconhecido",
            "model": "não definido",
        }


__all__ = ["QueryAnalyzerAgent"]
