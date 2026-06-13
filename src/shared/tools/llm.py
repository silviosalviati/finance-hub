from __future__ import annotations

import time
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI

from src.shared.config import (
    LLM_PROVIDER,
    VERTEXAI_LOCATION,
    VERTEXAI_MAX_OUTPUT_TOKENS,
    VERTEXAI_MAX_RETRIES,
    VERTEXAI_MODEL,
    VERTEXAI_PROJECT,
    VERTEXAI_TEMPERATURE,
)


def create_vertexai_llm() -> BaseChatModel:
    return ChatGoogleGenerativeAI(
        model=VERTEXAI_MODEL,
        vertexai=True,
        project=VERTEXAI_PROJECT,
        location=VERTEXAI_LOCATION,
        temperature=VERTEXAI_TEMPERATURE,
        max_tokens=VERTEXAI_MAX_OUTPUT_TOKENS,
        max_retries=VERTEXAI_MAX_RETRIES,
    )


def create_llm() -> BaseChatModel:
    if LLM_PROVIDER == "vertexai":
        return create_vertexai_llm()

    raise ValueError(
        f"Provedor configurado nao suportado neste ambiente: {LLM_PROVIDER}. "
        "Atualmente o wrapper ativo usa Vertex AI."
    )


def invoke_with_retry(
    llm: BaseChatModel,
    messages: list,
    max_attempts: int = 3,
    base_delay: float = 2.0,
) -> Any:
    """Invoca o LLM com retry exponencial para erros transitórios.

    Tenta até max_attempts vezes com backoff exponencial entre tentativas.
    Levanta a última exceção se todas as tentativas falharem.
    """
    last_exc: BaseException = RuntimeError("Nenhuma tentativa realizada")
    for attempt in range(max_attempts):
        try:
            return llm.invoke(messages)
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                time.sleep(base_delay * (2 ** attempt))
    raise last_exc
