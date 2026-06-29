from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any

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


def invoke_with_retry(
    llm: BaseChatModel,
    messages: list,
    max_attempts: int = 3,
    base_delay: float = 2.0,
    label: str = "",
) -> Any:
    """Retry síncrono — usar apenas em contextos síncronos (nós LangGraph em thread executor).

    `label` identifica o chamador (ex.: "planner", "composer", "text_to_sql")
    nos logs de tempo — sem isso não há como saber, em produção, qual etapa
    do pipeline está realmente pesando no tempo de resposta (ver PRD em
    docs/plans/2026-06-21-tempo-resposta-prd.md).
    """
    last_exc: BaseException = RuntimeError("Nenhuma tentativa realizada")
    started_at = time.perf_counter()
    for attempt in range(max_attempts):
        try:
            result = llm.invoke(messages)
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
) -> Any:
    """Retry assíncrono — não bloqueia o event loop do FastAPI.

    Ver docstring de `invoke_with_retry` sobre `label`.
    """
    last_exc: BaseException = RuntimeError("Nenhuma tentativa realizada")
    started_at = time.perf_counter()
    for attempt in range(max_attempts):
        try:
            result = await llm.ainvoke(messages)
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
