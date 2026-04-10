from __future__ import annotations

import json
import re
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.query_analyzer.prompts import ANALYZE_SYSTEM_PROMPT
from src.agents.query_analyzer.state import AgentState
from src.shared.config import BQ_ANTIPATTERNS, BYTES_CRITICAL_THRESHOLD, BYTES_WARNING_THRESHOLD
from src.shared.tools.bigquery import dry_run_query, format_bytes, get_schemas_for_query
from src.shared.tools.schemas import OptimizationReport, QueryAntiPattern

TABLE_PATTERN = r"`?([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)`?"
SQL_FENCE_PATTERN = r"```sql\s*([\s\S]+?)\s*```"

SEVERITY_PENALTY = {
    "low": 5,
    "medium": 15,
    "high": 30,
    "critical": 50,
}


def parse_query(state: AgentState) -> dict:
    query_upper = state.original_query.upper()

    has_star = bool(re.search(r"\bSELECT\s+\*", query_upper))
    has_order_without_limit = bool(
        re.search(r"ORDER\s+BY", query_upper)
        and not re.search(r"LIMIT\s+\d+", query_upper)
    )
    has_cross_join = bool(re.search(r"CROSS\s+JOIN", query_upper))
    has_distinct = bool(re.search(r"\bDISTINCT\b", query_upper))
    has_union_without_all = bool(re.search(r"\bUNION\b(?!\s+ALL)", query_upper))

    join_count = len(re.findall(r"\bJOIN\b", query_upper))
    subquery_count = max(0, len(re.findall(r"\bSELECT\b", query_upper)) - 1)
    cte_count = len(re.findall(r"\bWITH\b", query_upper))

    tables = list(set(re.findall(TABLE_PATTERN, state.original_query)))

    structure = {
        "has_star": has_star,
        "has_order_without_limit": has_order_without_limit,
        "has_cross_join": has_cross_join,
        "has_distinct": has_distinct,
        "has_union_without_all": has_union_without_all,
        "join_count": join_count,
        "subquery_count": subquery_count,
        "cte_count": cte_count,
        "tables": tables,
    }

    return {"query_structure": structure}


def dry_run_estimate(state: AgentState) -> dict:
    result = dry_run_query(state.original_query, state.project_id)
    return {"dry_run_original": result}


def analyze_patterns(state: AgentState, llm: BaseChatModel) -> dict:
    structure = state.query_structure
    dry = state.dry_run_original

    cost_context = _build_cost_context(dry)
    schema_context = _safe_get_schema_context(state)
    antipatterns_list = "\n".join(f"- {pattern}" for pattern in BQ_ANTIPATTERNS)

    user_prompt = f"""
QUERY:
{state.original_query}

ANALISE ESTRUTURAL:
{json.dumps(structure, indent=2, ensure_ascii=False)}

CONTEXTO DE CUSTO:
{cost_context}

SCHEMA DISPONIVEL:
{schema_context}

ANTIPADROES POSSIVEIS:
{antipatterns_list}
"""

    antipatterns = _detect_antipatterns_with_llm(
        llm=llm,
        system_prompt=ANALYZE_SYSTEM_PROMPT,
        user_prompt=user_prompt,
    )

    forced_antipatterns = _build_forced_antipatterns(structure)
    antipatterns = _merge_antipatterns(antipatterns, forced_antipatterns)

    needs_optimization = bool(antipatterns)
    if dry and not dry.error and dry.bytes_processed > BYTES_WARNING_THRESHOLD:
        needs_optimization = True

    return {
        "antipatterns": antipatterns,
        "needs_optimization": needs_optimization,
    }


def optimize_query(state: AgentState, llm: BaseChatModel) -> dict:
    antipatterns_text = _build_antipatterns_text(state.antipatterns)
    cost_info = _build_optimization_cost_context(state.dry_run_original)
    is_complex_analytical = _is_complex_analytical_query(state)
    optimization_feedback = "\n".join(
        f"- {item}" for item in (state.optimization_feedback or [])
    )

    gold_layer_note = ""
    if is_complex_analytical:
        gold_layer_note = """
ATENCAO - Query analitica complexa detectada (possivel camada Gold/Silver):
- Esta query provavelmente gera KPIs e metricas para dashboards Power BI
- Foque APENAS em otimizacoes de infraestrutura (particoes, clustering, UNION ALL)
- NUNCA remova, renomeie ou reescreva colunas calculadas ou aliases de KPI
- NUNCA simplifique CASE WHEN ou logica de negocio
- Se houver SELECT *, MANTENHA-O se o schema nao estiver disponivel
"""

    system_prompt = f"""Voce e um especialista em BigQuery e SQL analitico para Power BI.
Otimize a query para reduzir bytes processados e slots consumidos no BigQuery.

REGRAS ABSOLUTAS - nunca viole nenhuma delas:
1. Preserve EXATAMENTE todas as colunas calculadas, aliases e metricas de KPI
2. Preserve TODAS as regras de negocio: CASE WHEN, IF, COALESCE, IIF, NULLIF
3. Preserve TODOS os JOINs necessarios
4. Preserve TODA a logica de datas, periodos e granularidade temporal
5. Preserve TODOS os filtros WHERE e HAVING
6. Remova SELECT * sempre que possivel e projete apenas colunas necessarias
7. Sempre que possivel, aplique filtro por particao/data para evitar full scan
8. O resultado DEVE ser identico ao original
9. Adicione comentarios -- explicando cada otimizacao
10. Responda APENAS com o SQL otimizado
{gold_layer_note}"""

    user_prompt = f"""## Query original:
```sql
{state.original_query}
```

Anti-padroes identificados para corrigir:

{antipatterns_text}

Contexto de custo (BigQuery dry-run):

{cost_info}

Feedback da iteracao anterior (quando houver):

{optimization_feedback or '(primeira tentativa de otimizacao)'}
"""

    response = llm.invoke(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
    )

    raw = _extract_message_content(response)
    optimized = _extract_sql_from_response(raw)

    return {
        "optimized_query": optimized,
        "iteration": state.iteration + 1,
    }


def validate_optimized(state: AgentState) -> dict:
    if not state.optimized_query:
        return {}

    result = dry_run_query(state.optimized_query, state.project_id)
    feedback: list[str] = []
    needs_optimization = False

    if result.error:
        needs_optimization = True
        feedback.append(f"Dry-run da query otimizada falhou: {result.error}")
    else:
        optimized_structure = _inspect_query_structure(state.optimized_query)

        if optimized_structure["has_star"]:
            needs_optimization = True
            feedback.append("A query otimizada ainda usa SELECT *.")

        if optimized_structure["has_cross_join"]:
            needs_optimization = True
            feedback.append("A query otimizada ainda possui CROSS JOIN.")

        if optimized_structure["has_order_without_limit"]:
            needs_optimization = True
            feedback.append("A query otimizada manteve ORDER BY sem LIMIT.")

        dry_orig = state.dry_run_original
        if dry_orig and not dry_orig.error and dry_orig.bytes_processed > 0:
            if result.bytes_processed >= dry_orig.bytes_processed:
                needs_optimization = True
                feedback.append(
                    "A query otimizada nao reduziu bytes processados em relacao a original."
                )
            else:
                savings_pct = (
                    (dry_orig.bytes_processed - result.bytes_processed)
                    / dry_orig.bytes_processed
                    * 100
                )
                if savings_pct < 5:
                    needs_optimization = True
                    feedback.append(
                        "A economia de bytes ficou abaixo de 5%; busque reduzir full scan e leituras desnecessarias."
                    )

    return {
        "dry_run_optimized": result,
        "needs_optimization": needs_optimization,
        "optimization_feedback": feedback,
    }


def generate_report(state: AgentState, llm: BaseChatModel) -> dict:
    dry_orig = state.dry_run_original
    dry_opt = state.dry_run_optimized

    bytes_saved = None
    cost_saved = None
    savings_pct = None

    if dry_orig and dry_opt and not dry_orig.error and not dry_opt.error:
        bytes_saved = max(0, dry_orig.bytes_processed - dry_opt.bytes_processed)
        cost_saved = max(0.0, dry_orig.estimated_cost_usd - dry_opt.estimated_cost_usd)

        if dry_orig.bytes_processed > 0:
            savings_pct = round(bytes_saved / dry_orig.bytes_processed * 100, 1)

    score = _calculate_score(state)
    grade = _score_to_grade(score)
    power_bi_tips = _generate_power_bi_tips(state)

    summary = _generate_summary(
        llm=llm,
        state=state,
        score=score,
        grade=grade,
        savings_pct=savings_pct,
        dry_orig=dry_orig,
    )

    recommendations = [ap.suggestion for ap in state.antipatterns]
    if not recommendations:
        recommendations = ["Query esta seguindo boas praticas do BigQuery."]

    report = OptimizationReport(
        efficiency_score=score,
        grade=grade,
        summary=summary,
        antipatterns_found=state.antipatterns,
        optimized_query=state.optimized_query,
        bytes_saved=bytes_saved,
        cost_saved_usd=cost_saved,
        savings_pct=savings_pct,
        recommendations=recommendations,
        power_bi_tips=power_bi_tips,
    )

    return {"report": report}


def _safe_get_schema_context(state: AgentState) -> str:
    try:
        return get_schemas_for_query(state.original_query, state.project_id)
    except Exception:
        return "(detalhes de schema nao disponiveis)"


def _build_cost_context(dry: Any) -> str:
    if dry and not dry.error:
        return (
            f"Bytes Processados: {format_bytes(dry.bytes_processed)} | "
            f"Custo Estimado: USD {dry.estimated_cost_usd:.4f}"
        )

    if dry and dry.error:
        return f"Erro tecnico no dry-run: {dry.error}"

    return "Estimativa indisponivel (dry-run nao executado ou falhou)"


def _extract_message_content(response: Any) -> str:
    if hasattr(response, "content"):
        return str(response.content).strip()

    return str(response).strip()


def _strip_code_fences(raw: str) -> str:
    cleaned = raw.strip()
    cleaned = re.sub(r"^```(?:json|sql)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _extract_sql_from_response(raw: str) -> str:
    sql_match = re.search(SQL_FENCE_PATTERN, raw, re.IGNORECASE)
    if sql_match:
        return sql_match.group(1).strip()
    return _strip_code_fences(raw)


def _normalize_severity(value: str | None) -> str:
    if not value:
        return "MEDIUM"

    severity = value.strip().upper()
    if severity not in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}:
        return "MEDIUM"

    return severity


def _parse_antipatterns_json(raw: str) -> list[QueryAntiPattern]:
    cleaned = _strip_code_fences(raw)
    data = json.loads(cleaned)

    antipatterns: list[QueryAntiPattern] = []
    for ap in data.get("antipatterns", []):
        pattern = ap.get("pattern") or ap.get("name")
        if not pattern:
            continue

        antipatterns.append(
            QueryAntiPattern(
                pattern=pattern,
                description=ap.get("description", ""),
                severity=_normalize_severity(ap.get("severity")),
                suggestion=ap.get("suggestion", ""),
            )
        )

    return antipatterns


def _detect_antipatterns_with_llm(
    llm: BaseChatModel,
    system_prompt: str,
    user_prompt: str,
) -> list[QueryAntiPattern]:
    full_prompt = f"""<start_of_turn>user
{system_prompt}
{user_prompt}<end_of_turn>
<start_of_turn>model"""

    try:
        response = llm.invoke(full_prompt)
        raw = _extract_message_content(response)
        return _parse_antipatterns_json(raw)
    except Exception:
        return []


def _build_forced_antipatterns(structure: dict[str, Any]) -> list[QueryAntiPattern]:
    forced: list[QueryAntiPattern] = []

    if structure.get("has_star"):
        forced.append(
            QueryAntiPattern(
                pattern="SELECT *",
                description="Uso de SELECT * detectado via analise estrutural",
                severity="HIGH",
                suggestion="Projete apenas as colunas necessarias para reduzir leitura de dados",
            )
        )

    if structure.get("has_order_without_limit"):
        forced.append(
            QueryAntiPattern(
                pattern="ORDER BY sem LIMIT",
                description="ORDER BY global sem LIMIT detectado",
                severity="HIGH",
                suggestion="Adicione LIMIT ou remova ORDER BY para evitar ordenacao completa",
            )
        )

    if structure.get("has_cross_join"):
        forced.append(
            QueryAntiPattern(
                pattern="CROSS JOIN sem filtro",
                description="CROSS JOIN detectado via analise estrutural",
                severity="HIGH",
                suggestion="Evite CROSS JOIN sem filtro explicito",
            )
        )

    if structure.get("has_distinct"):
        forced.append(
            QueryAntiPattern(
                pattern="DISTINCT sem necessidade real",
                description="Uso de DISTINCT detectado via analise estrutural",
                severity="MEDIUM",
                suggestion="Valide se DISTINCT e realmente necessario",
            )
        )

    if structure.get("has_union_without_all"):
        forced.append(
            QueryAntiPattern(
                pattern="UNION sem ALL",
                description="UNION detectado sem ALL via analise estrutural",
                severity="MEDIUM",
                suggestion="Use UNION ALL quando nao for necessario remover duplicidades",
            )
        )

    return forced


def _merge_antipatterns(
    detected: list[QueryAntiPattern],
    forced: list[QueryAntiPattern],
) -> list[QueryAntiPattern]:
    merged = list(detected)
    existing = {ap.pattern.strip().lower() for ap in merged}

    for forced_item in forced:
        key = forced_item.pattern.strip().lower()
        if key not in existing:
            merged.append(forced_item)

    return merged


def _build_antipatterns_text(antipatterns: list[QueryAntiPattern]) -> str:
    if not antipatterns:
        return "Nenhum anti-padrao identificado."

    return "\n".join(
        f"[{ap.severity.upper()}] {ap.pattern}: {ap.description}"
        for ap in antipatterns
    )


def _build_optimization_cost_context(dry: Any) -> str:
    if dry and not dry.error:
        return (
            f"A query original processa {format_bytes(dry.bytes_processed)} "
            f"(USD {dry.estimated_cost_usd:.4f}). "
            "Aplique otimizacoes para reduzir ao maximo esses valores."
        )

    if dry and dry.error:
        return f"Dry-run original apresentou erro: {dry.error}"

    return "Custo original nao disponivel."


def _is_complex_analytical_query(state: AgentState) -> bool:
    structure = state.query_structure
    return (
        structure.get("cte_count", 0) >= 2
        or structure.get("join_count", 0) >= 3
        or structure.get("subquery_count", 0) >= 2
        or len(state.original_query) > 2000
    )


def _inspect_query_structure(query: str) -> dict[str, bool]:
    query_upper = query.upper()
    return {
        "has_star": bool(re.search(r"\bSELECT\s+\*", query_upper)),
        "has_order_without_limit": bool(
            re.search(r"ORDER\s+BY", query_upper)
            and not re.search(r"LIMIT\s+\d+", query_upper)
        ),
        "has_cross_join": bool(re.search(r"CROSS\s+JOIN", query_upper)),
    }


def _generate_summary(
    llm: BaseChatModel,
    state: AgentState,
    score: int,
    grade: str,
    savings_pct: float | None,
    dry_orig: Any,
) -> str:
    summary_prompt = f"""Em 2-3 frases em portugues, resuma a analise desta query BigQuery para Power BI.
Mencione: os principais problemas encontrados, o impacto em custo/performance, e a melhoria alcancada.

Anti-padroes: {len(state.antipatterns)} encontrado(s)
Score de eficiencia: {score}/100 ({grade})
Economia estimada: {f'{savings_pct}%' if savings_pct is not None else 'nao calculada'}
Query processava: {format_bytes(dry_orig.bytes_processed) if dry_orig else 'N/A'}
"""

    try:
        response = llm.invoke([HumanMessage(content=summary_prompt)])
        return _extract_message_content(response)
    except Exception:
        return (
            f"Query analisada. Score: {score}/100 ({grade}). "
            f"{len(state.antipatterns)} anti-padrao(s) detectado(s)."
        )


def _calculate_score(state: AgentState) -> int:
    score = 100

    dry_orig = state.dry_run_original
    dry_opt = state.dry_run_optimized
    penalty_multiplier = 1.0

    if (
        dry_orig
        and dry_opt
        and not dry_orig.error
        and not dry_opt.error
        and dry_opt.bytes_processed < dry_orig.bytes_processed
    ):
        penalty_multiplier = 0.5

    for antipattern in state.antipatterns:
        severity = (antipattern.severity or "").strip().lower()
        score -= int(SEVERITY_PENALTY.get(severity, 10) * penalty_multiplier)

    dry = dry_opt if (dry_opt and not dry_opt.error) else dry_orig
    if dry and not dry.error:
        if dry.bytes_processed > BYTES_CRITICAL_THRESHOLD:
            score -= 20
        elif dry.bytes_processed > BYTES_WARNING_THRESHOLD:
            score -= 10

    if (
        dry_orig
        and dry_opt
        and not dry_orig.error
        and not dry_opt.error
        and dry_orig.bytes_processed > 0
    ):
        savings_pct = (
            (dry_orig.bytes_processed - dry_opt.bytes_processed)
            / dry_orig.bytes_processed
            * 100
        )
        if savings_pct >= 50:
            score += 15
        elif savings_pct >= 30:
            score += 10
        elif savings_pct >= 15:
            score += 6
        elif savings_pct >= 5:
            score += 3

    return max(0, min(100, score))


def _score_to_grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def _generate_power_bi_tips(state: AgentState) -> list[str]:
    tips: list[str] = []
    structure = state.query_structure

    if structure.get("has_star"):
        tips.append(
            "No Power BI, projete apenas as colunas exibidas no dashboard - evite SELECT *."
        )

    if structure.get("has_order_without_limit"):
        tips.append(
            "ORDER BY sem LIMIT e inutil em muitos cenarios de consumo no Power BI."
        )

    if not tips:
        tips.append(
            "Use DirectQuery com filtro de particao para dashboards de series temporais."
        )
        tips.append(
            "Considere materializar agregacoes frequentes em scheduled queries."
        )

    return tips
