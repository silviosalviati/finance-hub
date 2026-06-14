from __future__ import annotations

import os

from src.shared.config import get_runtime_config


def configure_tracing() -> None:
    """Ativa LangSmith tracing se LANGCHAIN_API_KEY estiver configurado no DB."""
    api_key = get_runtime_config("LANGCHAIN_API_KEY", "")
    if not api_key:
        return

    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"] = api_key
    os.environ["LANGCHAIN_PROJECT"] = get_runtime_config("LANGCHAIN_PROJECT", "finance-hub")
    print(
        f"[Tracing] LangSmith ativado — projeto: {os.environ['LANGCHAIN_PROJECT']}"
    )
