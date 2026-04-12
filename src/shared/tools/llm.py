from __future__ import annotations

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
