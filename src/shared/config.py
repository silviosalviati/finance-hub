from __future__ import annotations

import os
import time as _time
from pathlib import Path

# Evita múltiplas queries SQLite por request — TTL de 30 segundos.
_config_cache: dict[str, tuple[str, float]] = {}
_CACHE_TTL = 30.0


def _from_db(key: str, default: str = "") -> str:
    """Lê do SQLite com cache TTL; cai em os.getenv(); usa default hardcoded."""
    now = _time.monotonic()
    cached = _config_cache.get(key)
    if cached is not None:
        value, ts = cached
        if now - ts < _CACHE_TTL:
            return value

    try:
        from src.core.database import get_config_value
        val = get_config_value(key, "")
        if val:
            _config_cache[key] = (val, now)
            return val
    except Exception:
        pass

    result = os.getenv(key, default)
    _config_cache[key] = (result, now)
    return result


def invalidate_config_cache(key: str | None = None) -> None:
    """Invalida cache de configuração. Sem argumento limpa tudo."""
    if key is None:
        _config_cache.clear()
    else:
        _config_cache.pop(key, None)


# ── Constantes de módulo (lidas na inicialização, usadas pelo CORS/FastAPI) ──
# Na primeira execução o DB ainda não existe → usa o default hardcoded.
# Nas execuções seguintes o DB já foi semeado e o valor vem do SQLite.

LLM_PROVIDER = _from_db("LLM_PROVIDER", "vertexai")

VERTEXAI_PROJECT = _from_db("VERTEXAI_PROJECT", "silviosalviati")
VERTEXAI_LOCATION = _from_db("VERTEXAI_LOCATION", "us-central1")
VERTEXAI_MODEL = _from_db("VERTEXAI_MODEL", "gemini-2.5-flash")
VERTEXAI_MAX_OUTPUT_TOKENS = int(_from_db("VERTEXAI_MAX_OUTPUT_TOKENS", "8192"))
VERTEXAI_MAX_RETRIES = int(_from_db("VERTEXAI_MAX_RETRIES", "1"))
VERTEXAI_TEMPERATURE = float(_from_db("VERTEXAI_TEMPERATURE", "0.05"))
LLM_TEMPERATURE = VERTEXAI_TEMPERATURE

SESSION_TTL_HOURS = int(_from_db("SESSION_TTL_HOURS", "8"))

ALLOWED_ORIGINS = [
    s.strip()
    for s in _from_db(
        "ALLOWED_ORIGINS",
        "http://localhost:8000,http://127.0.0.1:8000",
    ).split(",")
    if s.strip()
]

GCP_PROJECT_ID = _from_db("GCP_PROJECT_ID", "silviosalviati")
GCP_CREDENTIALS_PATH = _from_db(
    "GOOGLE_APPLICATION_CREDENTIALS",
    str(Path("secrets") / "credentials.json"),
)

FINANCE_AUDITOR_TABLE_REF = _from_db(
    "FINANCE_AUDITOR_TABLE_REF",
    "silviosalviati.ds_inteligencia_analitica.analitica_analise_ia",
)
FINANCE_AUDITOR_DEFAULT_PROJECT = _from_db(
    "FINANCE_AUDITOR_DEFAULT_PROJECT",
    "silviosalviati",
)

def get_gcp_project_ids() -> list[str]:
    """Retorna lista de project IDs GCP configurados (campo separado por vírgula no DB)."""
    raw = get_runtime_config("GCP_PROJECT_ID", "silviosalviati")
    return [p.strip() for p in raw.split(",") if p.strip()]


def get_default_gcp_project() -> str:
    """Retorna o primeiro project ID configurado como padrão."""
    projects = get_gcp_project_ids()
    return projects[0] if projects else ""


BQ_COST_PER_TB_USD = float(_from_db("BQ_COST_PER_TB_USD", "5.0"))
BYTES_WARNING_THRESHOLD = int(_from_db("BYTES_WARNING_THRESHOLD", str(10 * 1024**3)))
BYTES_CRITICAL_THRESHOLD = int(_from_db("BYTES_CRITICAL_THRESHOLD", str(100 * 1024**3)))

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

SUPPORTED_LLM_PROVIDERS = {"vertexai"}


def get_runtime_config(key: str, default: str = "") -> str:
    """Lê configuração em tempo de execução — sempre consulta o SQLite primeiro."""
    return _from_db(key, default)


def validate_runtime_config() -> list[str]:
    """Valida a configuração atual (lida do SQLite após init_db)."""
    errors: list[str] = []

    provider = get_runtime_config("LLM_PROVIDER", "vertexai")
    if provider not in SUPPORTED_LLM_PROVIDERS:
        errors.append(
            f"LLM_PROVIDER invalido: '{provider}'. "
            f"Use um de: {', '.join(sorted(SUPPORTED_LLM_PROVIDERS))}."
        )

    if provider == "vertexai":
        if not get_runtime_config("VERTEXAI_PROJECT"):
            errors.append("VERTEXAI_PROJECT nao configurado.")
        if not get_runtime_config("VERTEXAI_LOCATION"):
            errors.append("VERTEXAI_LOCATION nao configurado.")
        if not get_runtime_config("VERTEXAI_MODEL"):
            errors.append("VERTEXAI_MODEL nao configurado.")
        max_tokens = int(get_runtime_config("VERTEXAI_MAX_OUTPUT_TOKENS", "0"))
        if max_tokens <= 0:
            errors.append("VERTEXAI_MAX_OUTPUT_TOKENS deve ser maior que zero.")
        retries = int(get_runtime_config("VERTEXAI_MAX_RETRIES", "0"))
        if retries < 0:
            errors.append("VERTEXAI_MAX_RETRIES deve ser maior ou igual a zero.")
        temp = float(get_runtime_config("VERTEXAI_TEMPERATURE", "0"))
        if temp < 0:
            errors.append("VERTEXAI_TEMPERATURE deve ser maior ou igual a zero.")

    ttl = int(get_runtime_config("SESSION_TTL_HOURS", "0"))
    if ttl <= 0:
        errors.append("SESSION_TTL_HOURS deve ser maior que zero.")

    if not get_runtime_config("ALLOWED_ORIGINS"):
        errors.append("ALLOWED_ORIGINS nao pode ficar vazio.")

    gcp_project = get_runtime_config("GCP_PROJECT_ID")
    if not gcp_project or not get_gcp_project_ids():
        errors.append("GCP_PROJECT_ID nao configurado.")

    credentials_path = get_runtime_config(
        "GOOGLE_APPLICATION_CREDENTIALS",
        str(Path("secrets") / "credentials.json"),
    )
    if not credentials_path:
        errors.append("GOOGLE_APPLICATION_CREDENTIALS nao configurado.")
    else:
        creds = Path(credentials_path)
        if not creds.exists():
            errors.append(f"Arquivo de credenciais nao encontrado: {credentials_path}")
        elif not creds.is_file():
            errors.append(f"GOOGLE_APPLICATION_CREDENTIALS nao é um arquivo: {credentials_path}")

    bq_warn = int(get_runtime_config("BYTES_WARNING_THRESHOLD", "0"))
    bq_crit = int(get_runtime_config("BYTES_CRITICAL_THRESHOLD", "0"))
    if bq_warn <= 0:
        errors.append("BYTES_WARNING_THRESHOLD deve ser maior que zero.")
    if bq_crit <= 0:
        errors.append("BYTES_CRITICAL_THRESHOLD deve ser maior que zero.")
    if bq_crit > 0 and bq_warn > 0 and bq_crit < bq_warn:
        errors.append("BYTES_CRITICAL_THRESHOLD deve ser >= BYTES_WARNING_THRESHOLD.")

    return errors


def print_runtime_summary() -> None:
    provider = get_runtime_config("LLM_PROVIDER", "vertexai")
    print(f"LLM_PROVIDER: {provider}")
    if provider == "vertexai":
        print(f"VERTEXAI_PROJECT:            {get_runtime_config('VERTEXAI_PROJECT')}")
        print(f"VERTEXAI_LOCATION:           {get_runtime_config('VERTEXAI_LOCATION')}")
        print(f"VERTEXAI_MODEL:              {get_runtime_config('VERTEXAI_MODEL')}")
        print(f"VERTEXAI_MAX_OUTPUT_TOKENS:  {get_runtime_config('VERTEXAI_MAX_OUTPUT_TOKENS')}")
        print(f"VERTEXAI_MAX_RETRIES:        {get_runtime_config('VERTEXAI_MAX_RETRIES')}")
        print(f"VERTEXAI_TEMPERATURE:        {get_runtime_config('VERTEXAI_TEMPERATURE')}")
    print(f"GCP_PROJECT_ID:              {get_runtime_config('GCP_PROJECT_ID')}")
    print(f"GOOGLE_APPLICATION_CREDENTIALS: {get_runtime_config('GOOGLE_APPLICATION_CREDENTIALS')}")
    print(f"SESSION_TTL_HOURS:           {get_runtime_config('SESSION_TTL_HOURS')}")
    print(f"ALLOWED_ORIGINS:             {get_runtime_config('ALLOWED_ORIGINS')}")
