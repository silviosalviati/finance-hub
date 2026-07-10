from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any

from langchain_core.callbacks.usage import get_usage_metadata_callback
from langchain_core.language_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI

from src.shared.config import get_runtime_config, get_vertexai_project

_logger = logging.getLogger(__name__)


def _ensure_google_adc_env() -> None:
    """Garante GOOGLE_APPLICATION_CREDENTIALS para SDKs que usam ADC.

    O runtime config pode estar no SQLite mesmo quando a variavel de ambiente
    nao foi exportada na sessao do processo.
    """
    configured = get_runtime_config("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not configured:
        return

    candidate = Path(configured).expanduser()
    if not candidate.is_absolute():
        candidate = (Path(__file__).resolve().parents[3] / candidate).resolve()

    if candidate.exists() and candidate.is_file():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(candidate)


def create_llm(temperature: float | None = None) -> BaseChatModel:
    """Cria um LLM com temperatura opcional.

    Args:
        temperature: Sobrescreve VERTEXAI_TEMPERATURE do DB quando informado.
                     Use None para temperatura analítica padrão (baixa, precisa).
                     Passe VERTEXAI_TEMPERATURE_CREATIVE para tarefas criativas.
    """
    provider = get_runtime_config("LLM_PROVIDER", "vertexai")
    if provider == "vertexai":
        _ensure_google_adc_env()
        t = temperature if temperature is not None else float(
            get_runtime_config("VERTEXAI_TEMPERATURE", "0.05")
        )
        return ChatGoogleGenerativeAI(
            model=get_runtime_config("VERTEXAI_MODEL", "gemini-2.5-flash"),
            vertexai=True,
            project=get_vertexai_project(),
            location=get_runtime_config("VERTEXAI_LOCATION", "us-central1"),
            temperature=t,
            max_tokens=int(get_runtime_config("VERTEXAI_MAX_OUTPUT_TOKENS", "8192")),
            max_retries=int(get_runtime_config("VERTEXAI_MAX_RETRIES", "1")),
        )

    raise ValueError(
        f"Provedor configurado nao suportado neste ambiente: {provider}. "
        "Atualmente o wrapper ativo usa Vertex AI."
    )


def _record_usage(
    usage_sink: list[dict[str, Any]] | None,
    label: str,
    usage_by_model: dict[str, dict[str, int]] | None,
) -> None:
    """Anexa o uso real de tokens (por modelo) de UMA chamada em `usage_sink`,
    rotulado com `label`. `usage_sink` é uma lista mutável compartilhada
    (ver `SupervisorState.usage_log`) — múltiplas capabilities podem rodar em
    threads paralelas (`node_router`'s `ThreadPoolExecutor`) e fazer append
    concorrente; isso é seguro em CPython porque `list.append` é atômico sob
    o GIL, sem necessidade de lock explícito.
    """
    if usage_sink is None or not usage_by_model:
        return
    for model_name, usage in usage_by_model.items():
        usage_sink.append({
            "label": label or "unlabeled",
            "model": model_name,
            "input_tokens": int(usage.get("input_tokens") or 0),
            "output_tokens": int(usage.get("output_tokens") or 0),
            "total_tokens": int(usage.get("total_tokens") or 0),
        })


def summarize_usage_by_label(usage_log: list[dict[str, Any]] | None) -> dict[str, dict[str, int]]:
    """Agrega `usage_log` (lista de eventos por chamada) em totais por label
    (nó/capability) — usado pelo audit log para saber onde o custo de LLM
    está concentrado, algo que o agregado por-modelo do
    `get_usage_metadata_callback` sozinho não permite responder."""
    by_label: dict[str, dict[str, int]] = {}
    for entry in usage_log or []:
        label = entry.get("label") or "unlabeled"
        agg = by_label.setdefault(
            label, {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0, "calls": 0}
        )
        agg["input_tokens"] += int(entry.get("input_tokens") or 0)
        agg["output_tokens"] += int(entry.get("output_tokens") or 0)
        agg["total_tokens"] += int(entry.get("total_tokens") or 0)
        agg["calls"] += 1
    return by_label


def invoke_with_retry(
    llm: BaseChatModel,
    messages: list,
    max_attempts: int = 3,
    base_delay: float = 2.0,
    label: str = "",
    usage_sink: list[dict[str, Any]] | None = None,
) -> Any:
    """Retry síncrono — usar apenas em contextos síncronos (nós LangGraph em thread executor).

    `label` identifica o chamador (ex.: "planner", "composer", "text_to_sql")
    nos logs de tempo — sem isso não há como saber, em produção, qual etapa
    do pipeline está realmente pesando no tempo de resposta (ver PRD em
    docs/plans/2026-06-21-tempo-resposta-prd.md).

    `usage_sink`, se informado, recebe o uso real de tokens desta chamada
    (via callback de provider — funciona mesmo com structured output, que
    devolve o objeto já parseado sem `.usage_metadata` acessível). Permite
    reconstruir consumo de tokens por nó, não só o agregado por modelo.
    """
    last_exc: BaseException = RuntimeError("Nenhuma tentativa realizada")
    started_at = time.perf_counter()
    for attempt in range(max_attempts):
        try:
            with get_usage_metadata_callback() as usage_cb:
                result = llm.invoke(messages)
            _record_usage(usage_sink, label, usage_cb.usage_metadata)
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            _logger.info(
                "[llm_timing] label=%s ms=%.0f attempts=%d",
                label or "unlabeled", elapsed_ms, attempt + 1,
            )
            return result
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                time.sleep(base_delay * (2 ** attempt))
    elapsed_ms = (time.perf_counter() - started_at) * 1000
    _logger.info(
        "[llm_timing] label=%s ms=%.0f attempts=%d status=failed",
        label or "unlabeled", elapsed_ms, max_attempts,
    )
    raise last_exc


async def invoke_with_retry_async(
    llm: BaseChatModel,
    messages: list,
    max_attempts: int = 3,
    base_delay: float = 2.0,
    label: str = "",
    usage_sink: list[dict[str, Any]] | None = None,
) -> Any:
    """Retry assíncrono — não bloqueia o event loop do FastAPI.

    Ver docstring de `invoke_with_retry` sobre `label`/`usage_sink`.
    """
    last_exc: BaseException = RuntimeError("Nenhuma tentativa realizada")
    started_at = time.perf_counter()
    for attempt in range(max_attempts):
        try:
            with get_usage_metadata_callback() as usage_cb:
                result = await llm.ainvoke(messages)
            _record_usage(usage_sink, label, usage_cb.usage_metadata)
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            _logger.info(
                "[llm_timing] label=%s ms=%.0f attempts=%d",
                label or "unlabeled", elapsed_ms, attempt + 1,
            )
            return result
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))
    elapsed_ms = (time.perf_counter() - started_at) * 1000
    _logger.info(
        "[llm_timing] label=%s ms=%.0f attempts=%d status=failed",
        label or "unlabeled", elapsed_ms, max_attempts,
    )
    raise last_exc
