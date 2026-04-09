from __future__ import annotations

from langchain_core.language_models import BaseChatModel
from langchain_huggingface import ChatHuggingFace, HuggingFaceEndpoint

from src.shared.config import (
    HF_API_TOKEN,
    HF_MAX_NEW_TOKENS,
    HF_MODEL_ID,
    HF_TEMPERATURE,
    LLM_PROVIDER,
)


def create_huggingface_llm() -> BaseChatModel:
    base_llm = HuggingFaceEndpoint(
        repo_id=HF_MODEL_ID,
        huggingfacehub_api_token=HF_API_TOKEN,
        max_new_tokens=HF_MAX_NEW_TOKENS,
        temperature=HF_TEMPERATURE,
        task="conversational",
    )
    return ChatHuggingFace(llm=base_llm)


def create_llm() -> BaseChatModel:
    if LLM_PROVIDER == "huggingface":
        return create_huggingface_llm()

    raise ValueError(
        f"Provedor configurado nao suportado neste ambiente: {LLM_PROVIDER}. "
        "Atualmente o wrapper ativo usa HuggingFace."
    )
