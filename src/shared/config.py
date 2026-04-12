from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(override=True)


def _get_required_str(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Variavel obrigatoria nao configurada: {name}")
    return value.strip()


def _get_optional_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Variavel {name} deve ser um inteiro valido.") from exc


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f"Variavel {name} deve ser um numero valido.") from exc


def _get_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


LLM_PROVIDER = _get_required_str("LLM_PROVIDER").lower()

HF_API_TOKEN = _get_optional_str("HF_API_TOKEN")
HF_MODEL_ID = _get_optional_str("HF_MODEL_ID")
HF_MAX_NEW_TOKENS = _get_int("HF_MAX_NEW_TOKENS", 4096)
HF_TEMPERATURE = _get_float("HF_TEMPERATURE", 0.05)

OPENAI_API_KEY = _get_optional_str("OPENAI_API_KEY")
OPENAI_MODEL = _get_optional_str("OPENAI_MODEL", "gpt-4o")

VERTEXAI_PROJECT = _get_optional_str("VERTEXAI_PROJECT")
VERTEXAI_LOCATION = _get_optional_str("VERTEXAI_LOCATION", "us-central1")
VERTEXAI_MODEL = _get_optional_str("VERTEXAI_MODEL", "gemini-1.5-pro")

LLM_TEMPERATURE = HF_TEMPERATURE

SESSION_TTL_HOURS = _get_int("SESSION_TTL_HOURS", 8)

ALLOWED_ORIGINS = _get_list(
    "ALLOWED_ORIGINS",
    [
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
)

GCP_PROJECT_ID = _get_required_str("GCP_PROJECT_ID")
GCP_CREDENTIALS_PATH = _get_required_str("GOOGLE_APPLICATION_CREDENTIALS")

BQ_COST_PER_TB_USD = _get_float("BQ_COST_PER_TB_USD", 5.0)

BYTES_WARNING_THRESHOLD = _get_int("BYTES_WARNING_THRESHOLD", 10 * 1024**3)
BYTES_CRITICAL_THRESHOLD = _get_int("BYTES_CRITICAL_THRESHOLD", 100 * 1024**3)

BQ_ANTIPATTERNS = [
    "SELECT *",
    "CROSS JOIN sem filtro",
    "Subquery correlacionada sem materializacao",
    "Funcoes nao-deterministicas em WHERE (NOW, RAND)",
    "DISTINCT sem necessidade real",
    "ORDER BY sem LIMIT",
    "Ausencia de filtro em coluna de particao",
    "JOIN em coluna nao clusterizada",
    "Multiplos CTEs sem reaproveitamento",
    "UNION ALL vs UNION desnecessario",
    "Conversao implicita de tipos em JOIN",
    "Uso de REGEXP onde LIKE basta",
    "Agregacao sobre SELECT * antes de filtrar",
]

SUPPORTED_LLM_PROVIDERS = {"huggingface", "openai", "vertexai"}


def validate_runtime_config() -> list[str]:
    errors: list[str] = []

    if LLM_PROVIDER not in SUPPORTED_LLM_PROVIDERS:
        errors.append(
            f"LLM_PROVIDER invalido: {LLM_PROVIDER}. "
            f"Use um de: {', '.join(sorted(SUPPORTED_LLM_PROVIDERS))}."
        )

    if LLM_PROVIDER == "huggingface":
        if not HF_API_TOKEN:
            errors.append("HF_API_TOKEN nao configurado.")
        if not HF_MODEL_ID:
            errors.append("HF_MODEL_ID nao configurado.")

    if LLM_PROVIDER == "openai":
        if not OPENAI_API_KEY:
            errors.append("OPENAI_API_KEY nao configurado.")
        if not OPENAI_MODEL:
            errors.append("OPENAI_MODEL nao configurado.")

    if LLM_PROVIDER == "vertexai":
        if not VERTEXAI_PROJECT:
            errors.append("VERTEXAI_PROJECT nao configurado.")
        if not VERTEXAI_LOCATION:
            errors.append("VERTEXAI_LOCATION nao configurado.")
        if not VERTEXAI_MODEL:
            errors.append("VERTEXAI_MODEL nao configurado.")

    if SESSION_TTL_HOURS <= 0:
        errors.append("SESSION_TTL_HOURS deve ser maior que zero.")

    if not ALLOWED_ORIGINS:
        errors.append("ALLOWED_ORIGINS nao pode ficar vazio.")

    if not GCP_PROJECT_ID:
        errors.append("GCP_PROJECT_ID nao configurado.")

    if not GCP_CREDENTIALS_PATH:
        errors.append("GOOGLE_APPLICATION_CREDENTIALS nao configurado.")
    else:
        credentials_path = Path(GCP_CREDENTIALS_PATH)
        if not credentials_path.exists():
            errors.append(f"Arquivo de credenciais nao encontrado: {GCP_CREDENTIALS_PATH}")
        elif not credentials_path.is_file():
            errors.append(
                "GOOGLE_APPLICATION_CREDENTIALS nao aponta para um arquivo valido: "
                f"{GCP_CREDENTIALS_PATH}"
            )

    if BYTES_WARNING_THRESHOLD <= 0:
        errors.append("BYTES_WARNING_THRESHOLD deve ser maior que zero.")

    if BYTES_CRITICAL_THRESHOLD <= 0:
        errors.append("BYTES_CRITICAL_THRESHOLD deve ser maior que zero.")

    if BYTES_CRITICAL_THRESHOLD < BYTES_WARNING_THRESHOLD:
        errors.append(
            "BYTES_CRITICAL_THRESHOLD deve ser maior ou igual a BYTES_WARNING_THRESHOLD."
        )

    return errors


def print_runtime_summary() -> None:
    print("CONFIG")
    print(f"LLM_PROVIDER: {LLM_PROVIDER}")

    if LLM_PROVIDER == "huggingface":
        print(f"HF_MODEL_ID: {HF_MODEL_ID}")
        print(f"HF_MAX_NEW_TOKENS: {HF_MAX_NEW_TOKENS}")
        print(f"HF_TEMPERATURE: {HF_TEMPERATURE}")
    elif LLM_PROVIDER == "openai":
        print(f"OPENAI_MODEL: {OPENAI_MODEL}")
    elif LLM_PROVIDER == "vertexai":
        print(f"VERTEXAI_PROJECT: {VERTEXAI_PROJECT}")
        print(f"VERTEXAI_LOCATION: {VERTEXAI_LOCATION}")
        print(f"VERTEXAI_MODEL: {VERTEXAI_MODEL}")

    print(f"GCP_PROJECT_ID: {GCP_PROJECT_ID}")
    print(
        "GOOGLE_APPLICATION_CREDENTIALS: "
        f"{GCP_CREDENTIALS_PATH if Path(GCP_CREDENTIALS_PATH).exists() else 'arquivo nao encontrado'}"
    )
    print(f"SESSION_TTL_HOURS: {SESSION_TTL_HOURS}")
    print(f"ALLOWED_ORIGINS: {ALLOWED_ORIGINS}")
