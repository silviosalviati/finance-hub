from __future__ import annotations

from langchain_core.language_models import BaseChatModel
from langchain_google_vertexai import ChatVertexAI

from src.shared.config import (
    LLM_PROVIDER,
    VERTEXAI_LOCATION,
    VERTEXAI_MAX_OUTPUT_TOKENS,
    VERTEXAI_MODEL,
    VERTEXAI_PROJECT,
    VERTEXAI_TEMPERATURE,
)


def create_vertexai_llm() -> BaseChatModel:
    return ChatVertexAI(
        model=VERTEXAI_MODEL,
        project=VERTEXAI_PROJECT,
        location=VERTEXAI_LOCATION,
        temperature=VERTEXAI_TEMPERATURE,
        max_output_tokens=VERTEXAI_MAX_OUTPUT_TOKENS,
    )


def create_llm() -> BaseChatModel:
    if LLM_PROVIDER == "vertexai":
        return create_vertexai_llm()

    raise ValueError(
        f"Provedor configurado nao suportado neste ambiente: {LLM_PROVIDER}. "
        "Atualmente o wrapper ativo usa Vertex AI."
    )
