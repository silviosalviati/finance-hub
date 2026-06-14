from __future__ import annotations

import asyncio
import time
from functools import lru_cache
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI

from src.shared.config import get_runtime_config


def create_vertexai_llm() -> BaseChatModel:
    return ChatGoogleGenerativeAI(
        model=get_runtime_config("VERTEXAI_MODEL", "gemini-2.5-flash"),
        vertexai=True,
        project=get_runtime_config("VERTEXAI_PROJECT", "silviosalviati"),
        location=get_runtime_config("VERTEXAI_LOCATION", "us-central1"),
        temperature=float(get_runtime_config("VERTEXAI_TEMPERATURE", "0.05")),
        max_tokens=int(get_runtime_config("VERTEXAI_MAX_OUTPUT_TOKENS", "8192")),
        max_retries=int(get_runtime_config("VERTEXAI_MAX_RETRIES", "1")),
    )


def create_llm() -> BaseChatModel:
    provider = get_runtime_config("LLM_PROVIDER", "vertexai")
    if provider == "vertexai":
        return create_vertexai_llm()

    raise ValueError(
        f"Provedor configurado nao suportado neste ambiente: {provider}. "
        "Atualmente o wrapper ativo usa Vertex AI."
    )


def invoke_with_retry(
    llm: BaseChatModel,
    messages: list,
    max_attempts: int = 3,
    base_delay: float = 2.0,
) -> Any:
    """Retry síncrono — usar apenas em contextos síncronos (nós LangGraph em thread executor)."""
    last_exc: BaseException = RuntimeError("Nenhuma tentativa realizada")
    for attempt in range(max_attempts):
        try:
            return llm.invoke(messages)
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                time.sleep(base_delay * (2 ** attempt))
    raise last_exc


async def invoke_with_retry_async(
    llm: BaseChatModel,
    messages: list,
    max_attempts: int = 3,
    base_delay: float = 2.0,
) -> Any:
    """Retry assíncrono — não bloqueia o event loop do FastAPI."""
    last_exc: BaseException = RuntimeError("Nenhuma tentativa realizada")
    for attempt in range(max_attempts):
        try:
            return await llm.ainvoke(messages)
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))
    raise last_exc
