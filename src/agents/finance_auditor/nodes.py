"""Nós do grafo FinanceAuditor (VoC + Análise de Fricção).

Dependências de execução:
    fetch_data  →  node_sentiment  ─┐
                →  node_friction   ─┼→  consolidate_metrics  →  report_generator
                →  node_themes     ─┘
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import date, timedelta
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.finance_auditor.prompts import (
    CATEGORIZE_THEMES_PROMPT,
    EXTRACT_DATE_RANGE_PROMPT,
    REPORT_GENERATOR_PROMPT,
)
from src.agents.finance_auditor.state import (
    DEFAULT_PROJECT,
    TABLE_REF,
    FinanceAuditorState,
)
from src.shared.tools.bigquery import execute_query_rows

# Número máximo de linhas buscadas para análise
_MAX_ROWS = 500
# Colunas selecionadas na query de dados
_DATA_COLUMNS = (
    "SENTIMENTO_CLIENTE",
    "COALESCE(TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS, 0)"
    " AS TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS",
    "CONVERSA_E_RECHAMADA",
    "PALAVRAS_CHAVE",
    "ASSUNTO",
)

_CURRENT_MONTH_PATTERN = re.compile(
    r"\b(m[eê]s\s+atual|m[eê]s\s+corrente|este\s+m[eê]s)\b",
    re.IGNORECASE,
)
_LAST_MONTH_PATTERN = re.compile(
    r"\b(m[eê]s\s+passado|m[eê]s\s+anterior|[uú]ltimo\s+m[eê]s)\b",
    re.IGNORECASE,
)
_LAST_DAYS_PATTERN = re.compile(
    r"\b([uú]ltimos?)\s+(\d{1,3})\s+dias\b",
    re.IGNORECASE,
)
_MONTH_YEAR_PATTERN = re.compile(
    r"\bm[eê]s\s+de\s+([a-zA-ZÀ-ÿçÇ]+)\s+de\s+(\d{4})\b",
    re.IGNORECASE,
)

_MONTHS_PT: dict[str, int] = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _text(response: Any) -> str:
    """Extrai o texto bruto de uma resposta do LLM."""
    if hasattr(response, "content"):
        return str(response.content)
    return str(response)


def _parse_json_safe(raw: str) -> dict[str, Any]:
    """Tenta extrair e parsear o primeiro bloco JSON do texto."""
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return {}


def _default_period() -> tuple[str, str]:
    today = date.today()
    return (today - timedelta(days=30)).isoformat(), today.isoformat()


def _strip_accents(text: str) -> str:
    return "".join(
        c
        for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    start = date(year=year, month=month, day=1)
    if month == 12:
        next_month = date(year=year + 1, month=1, day=1)
    else:
        next_month = date(year=year, month=month + 1, day=1)
    end = next_month - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def _current_month_period(today: date | None = None) -> tuple[str, str]:
    """Retorna o período completo do mês atual (1º dia ao último dia)."""
    d = today or date.today()
    return _month_bounds(d.year, d.month)


def _previous_month_period(today: date | None = None) -> tuple[str, str]:
    d = today or date.today()
    if d.month == 1:
        return _month_bounds(d.year - 1, 12)
    return _month_bounds(d.year, d.month - 1)


def _deterministic_period_from_text(
    text: str,
    today: date | None = None,
) -> tuple[str, str] | None:
    """Extrai períodos determinísticos para termos comuns em português."""
    content = text or ""
    if _CURRENT_MONTH_PATTERN.search(content):
        return _current_month_period(today)

    if _LAST_MONTH_PATTERN.search(content):
        return _previous_month_period(today)

    last_days_match = _LAST_DAYS_PATTERN.search(content)
    if last_days_match:
        d = today or date.today()
        days = int(last_days_match.group(2))
        start = d - timedelta(days=days)
        return start.isoformat(), d.isoformat()

    month_year_match = _MONTH_YEAR_PATTERN.search(content)
    if month_year_match:
        month_token = _strip_accents(month_year_match.group(1).strip().lower())
        year = int(month_year_match.group(2))
        month = _MONTHS_PT.get(month_token)
        if month:
            return _month_bounds(year, month)

    return None


def _build_fallback_report(state: FinanceAuditorState) -> str:
    """Gera relatório básico em Markdown quando o LLM falha."""
    period = (
        f"{state.get('date_filter_start', '?')} a {state.get('date_filter_end', '?')}"
    )
    total = state.get("total_records", 0)
    score = state.get("friction_score", 0.0)
    label = state.get("friction_label", "N/A")
    error = state.get("error")

    lines = [
        f"# Relatório VoC — {period}",
        "",
        "## Resumo Executivo",
        "",
    ]
    if error:
        lines += [
            f"> **Erro durante análise:** {error}",
            "",
            "Não foi possível gerar o relatório completo.",
        ]
    else:
        lines += [
            f"- **Registros no período:** {total:,}",
            f"- **Índice de Fricção:** {score:.1%} ({label})",
        ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Nó 1 — fetch_data
# ---------------------------------------------------------------------------


def fetch_data(state: FinanceAuditorState, llm: BaseChatModel) -> dict[str, Any]:
    """Extrai período e busca dados na tabela de análise de IA."""
    try:
        request_text = state.get("request_text") or ""

        # 1. Regras determinísticas para evitar períodos incorretos vindos do LLM
        deterministic = _deterministic_period_from_text(request_text)
        if deterministic is not None:
            date_start, date_end = deterministic
        else:
            # 2. LLM extrai range de datas
            raw_response = llm.invoke(
                [
                    SystemMessage(content=EXTRACT_DATE_RANGE_PROMPT),
                    HumanMessage(content=request_text),
                ]
            )
            date_json = _parse_json_safe(_text(raw_response))

            default_start, default_end = _default_period()
            date_start = date_json.get("date_start") or default_start
            date_end = date_json.get("date_end") or default_end

        project = state.get("project_id") or DEFAULT_PROJECT

        # 2. Query de contagem total
        count_sql = f"""
            SELECT COUNT(*) AS total
            FROM `{TABLE_REF}`
            WHERE Data_registro_REF BETWEEN '{date_start}' AND '{date_end}'
        """
        count_rows = execute_query_rows(count_sql.strip(), project, max_rows=1)
        total_records = int(count_rows[0].get("total", 0)) if count_rows else 0

        # 3. Query de dados (amostra limitada)
        cols = ",\n                ".join(_DATA_COLUMNS)
        data_sql = f"""
            SELECT
                {cols}
            FROM `{TABLE_REF}`
            WHERE Data_registro_REF BETWEEN '{date_start}' AND '{date_end}'
            LIMIT {_MAX_ROWS}
        """
        raw_rows = execute_query_rows(data_sql.strip(), project, max_rows=_MAX_ROWS)

        warnings: list[str] = []
        if total_records > _MAX_ROWS:
            warnings.append(
                f"Amostra limitada a {_MAX_ROWS} de {total_records:,} registros."
            )

        return {
            "generated_sql": data_sql.strip(),
            "date_filter_start": date_start,
            "date_filter_end": date_end,
            "total_records": total_records,
            "raw_rows": raw_rows,
            "warnings": warnings,
        }

    except Exception as exc:  # noqa: BLE001
        return {
            "error": f"Falha ao buscar dados: {exc}",
            "generated_sql": "",
            "date_filter_start": "",
            "date_filter_end": "",
            "total_records": 0,
            "raw_rows": [],
            "warnings": [],
        }


# ---------------------------------------------------------------------------
# Nó 2a — node_sentiment  (paralelo, sem LLM)
# ---------------------------------------------------------------------------

_VALID_SENTIMENTS = ("POSITIVO", "NEGATIVO", "NEUTRO")


def node_sentiment(state: FinanceAuditorState) -> dict[str, Any]:
    """Analisa a distribuição de sentimento das interações."""
    raw_rows: list[dict[str, Any]] = state.get("raw_rows") or []

    counts: dict[str, int] = {s: 0 for s in _VALID_SENTIMENTS}
    counts["OUTROS"] = 0

    for row in raw_rows:
        val = str(row.get("SENTIMENTO_CLIENTE") or "").upper().strip()
        if val in counts:
            counts[val] += 1
        else:
            counts["OUTROS"] += 1

    total = len(raw_rows)
    distribution = {
        k: {"count": v, "pct": round(v / total * 100, 1) if total else 0.0}
        for k, v in counts.items()
    }
    dominant = max(counts, key=lambda k: counts[k]) if total else "N/A"

    return {
        "sentiment_result": {
            "total_sample": total,
            "counts": counts,
            "distribution": distribution,
            "dominant": dominant,
        }
    }


# ---------------------------------------------------------------------------
# Nó 2b — node_friction  (paralelo, sem LLM)
# ---------------------------------------------------------------------------

_WAIT_THRESHOLD_SECONDS = 300


def node_friction(state: FinanceAuditorState) -> dict[str, Any]:
    """Identifica pontos críticos de fricção: sentimento negativo + (espera > 5 min OU rechamada)."""
    raw_rows: list[dict[str, Any]] = state.get("raw_rows") or []

    friction_cases: list[dict[str, Any]] = []

    for row in raw_rows:
        sentiment = str(row.get("SENTIMENTO_CLIENTE") or "").upper().strip()
        wait = float(row.get("TEMPO_MEDIO_CLIENTE_ESPERANDO_SEGUNDOS") or 0)
        rechamada = str(row.get("CONVERSA_E_RECHAMADA") or "").upper().strip()

        long_wait = wait > _WAIT_THRESHOLD_SECONDS
        is_rechamada = rechamada == "SIM"

        if sentiment == "NEGATIVO" and (long_wait or is_rechamada):
            friction_cases.append(
                {
                    "sentimento": sentiment,
                    "tempo_espera_s": wait,
                    "rechamada": is_rechamada,
                    "assunto": row.get("ASSUNTO") or "",
                }
            )

    total = len(raw_rows)
    fc = len(friction_cases)
    long_wait_count = sum(1 for c in friction_cases if c["tempo_espera_s"] > _WAIT_THRESHOLD_SECONDS)
    rechamada_count = sum(1 for c in friction_cases if c["rechamada"])
    both_count = sum(
        1 for c in friction_cases if c["tempo_espera_s"] > _WAIT_THRESHOLD_SECONDS and c["rechamada"]
    )

    return {
        "friction_result": {
            "total_sample": total,
            "friction_count": fc,
            "friction_pct": round(fc / total * 100, 1) if total else 0.0,
            "breakdown": {
                "longa_espera": long_wait_count,
                "rechamada": rechamada_count,
                "ambos": both_count,
            },
            "sample_cases": friction_cases[:5],
        }
    }


# ---------------------------------------------------------------------------
# Nó 2c — node_themes  (paralelo, com LLM)
# ---------------------------------------------------------------------------

_THEMES_SAMPLE_SIZE = 80  # linhas enviadas ao LLM


def node_themes(state: FinanceAuditorState, llm: BaseChatModel) -> dict[str, Any]:
    """Categoriza os principais temas de contato usando LLM."""
    raw_rows: list[dict[str, Any]] = state.get("raw_rows") or []

    if not raw_rows:
        return {
            "themes_result": {
                "themes": [],
                "insights": "Sem dados disponíveis para análise de temas.",
            }
        }

    # Monta amostra textual para o LLM
    sample_lines = []
    for row in raw_rows[:_THEMES_SAMPLE_SIZE]:
        keywords = str(row.get("PALAVRAS_CHAVE") or "").strip()
        assunto = str(row.get("ASSUNTO") or "").strip()
        sentiment = str(row.get("SENTIMENTO_CLIENTE") or "").strip()
        if keywords or assunto:
            sample_lines.append(
                f"- Assunto: {assunto} | Palavras-chave: {keywords} | Sentimento: {sentiment}"
            )

    sample_text = "\n".join(sample_lines)

    try:
        response = llm.invoke(
            [
                SystemMessage(content=CATEGORIZE_THEMES_PROMPT),
                HumanMessage(content=sample_text),
            ]
        )
        data = _parse_json_safe(_text(response))
        if data.get("themes"):
            return {"themes_result": data}
    except Exception:  # noqa: BLE001
        pass

    # Fallback: frequência simples de palavras-chave
    keyword_counts: dict[str, int] = {}
    for row in raw_rows:
        for kw in str(row.get("PALAVRAS_CHAVE") or "").split(","):
            kw = kw.strip().lower()
            if len(kw) > 2:
                keyword_counts[kw] = keyword_counts.get(kw, 0) + 1

    top = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    themes = [
        {"nome": k, "frequencia_estimada": v, "sentimento_predominante": "N/A"}
        for k, v in top
    ]
    return {
        "themes_result": {
            "themes": themes,
            "insights": "Temas gerados por frequência de palavras-chave (fallback).",
        }
    }


# ---------------------------------------------------------------------------
# Nó 3 — consolidate_metrics  (fan-in)
# ---------------------------------------------------------------------------

_FRICTION_THRESHOLDS = (
    (0.30, "CRÍTICO"),
    (0.15, "ALTO"),
    (0.05, "MODERADO"),
    (0.00, "BAIXO"),
)


def consolidate_metrics(state: FinanceAuditorState) -> dict[str, Any]:
    """Consolida as métricas dos três nós paralelos em um único dicionário."""
    sentiment = state.get("sentiment_result") or {}
    friction = state.get("friction_result") or {}
    themes = state.get("themes_result") or {}

    total_sample: int = sentiment.get("total_sample", 0)
    friction_count: int = friction.get("friction_count", 0)
    friction_score = round(friction_count / total_sample, 4) if total_sample else 0.0

    friction_label = "BAIXO"
    for threshold, label in _FRICTION_THRESHOLDS:
        if friction_score >= threshold:
            friction_label = label
            break

    return {
        "friction_score": friction_score,
        "friction_label": friction_label,
        "consolidated_metrics": {
            "period": (
                f"{state.get('date_filter_start', '?')} a {state.get('date_filter_end', '?')}"
            ),
            "total_records": state.get("total_records", 0),
            "sample_size": total_sample,
            "friction_score": friction_score,
            "friction_pct": round(friction_score * 100, 2),
            "friction_label": friction_label,
            "sentiment_distribution": sentiment.get("distribution", {}),
            "dominant_sentiment": sentiment.get("dominant", "N/A"),
            "friction_breakdown": friction.get("breakdown", {}),
            "themes": themes.get("themes", []),
            "themes_insights": themes.get("insights", ""),
        },
    }


# ---------------------------------------------------------------------------
# Nó 4 — report_generator
# ---------------------------------------------------------------------------


def report_generator(state: FinanceAuditorState, llm: BaseChatModel) -> dict[str, Any]:
    """Gera o relatório executivo em Markdown usando LLM."""
    metrics = state.get("consolidated_metrics") or {}
    error = state.get("error")

    if error:
        return {
            "markdown_report": _build_fallback_report(state),
            "quality_score": 0,
        }

    try:
        metrics_json = json.dumps(metrics, ensure_ascii=False, indent=2)
        response = llm.invoke(
            [
                SystemMessage(content=REPORT_GENERATOR_PROMPT),
                HumanMessage(content=metrics_json),
            ]
        )
        data = _parse_json_safe(_text(response))
        report = data.get("markdown_report", "")
        quality = int(data.get("quality_score", 70))
        if report:
            return {"markdown_report": report, "quality_score": quality}
    except Exception:  # noqa: BLE001
        pass

    return {
        "markdown_report": _build_fallback_report(state),
        "quality_score": 60,
    }
