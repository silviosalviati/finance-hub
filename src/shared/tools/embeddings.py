"""Utilities de embedding compartilhadas — extraídas de catalog_index para
reutilização em agentic_retrieval, org_memory e outros módulos de retrieval.

Singleton do cliente Vertex AI — inicializado uma vez por processo.
"""

from __future__ import annotations

import math
from typing import Any

from src.shared.config import get_runtime_config, get_vertexai_project

_DEFAULT_EMBEDDING_MODEL = "text-embedding-005"
_embeddings_singleton: Any = None


def get_embeddings() -> Any:
    """Retorna o cliente de embeddings singleton (Vertex AI via ADC).

    Usa as mesmas credenciais de service account configuradas via ADC
    — nenhuma API key adicional necessária.
    """
    global _embeddings_singleton
    if _embeddings_singleton is None:
        from langchain_google_genai import GoogleGenerativeAIEmbeddings
        from src.shared.tools.llm import _ensure_google_adc_env

        _ensure_google_adc_env()
        _embeddings_singleton = GoogleGenerativeAIEmbeddings(
            model=get_runtime_config(
                "FINANCE_AUDITOR_EMBEDDING_MODEL", _DEFAULT_EMBEDDING_MODEL
            ),
            project=get_vertexai_project(),
            location=get_runtime_config("VERTEXAI_LOCATION", "us-central1"),
            vertexai=True,
        )
    return _embeddings_singleton


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Similaridade de cosseno entre dois vetores de ponto flutuante."""
    if len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if not norm_a or not norm_b:
        return 0.0
    return dot / (norm_a * norm_b)


__all__ = ["get_embeddings", "cosine_similarity"]
