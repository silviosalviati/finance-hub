from __future__ import annotations

import json
import re
import threading
import time
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.types import interrupt

from src.agents.query_analyzer.prompts import ANALYZE_SYSTEM_PROMPT
from src.agents.query_analyzer.state import AgentState
from src.core.database import get_dataset_memory, update_dataset_memory
from src.shared.config import BQ_ANTIPATTERNS, get_runtime_config
from src.shared.tools.bigquery import (
    dry_run_query,
    execute_query_rows,
    format_bytes,
    get_dataset_tables_schema,
    get_schemas_for_query,
)
from src.shared.tools.llm import invoke_with_retry
from src.shared.tools.schemas import (
    AntipatternList,
    IntelligenceReport,
    OptimizationReport,
    QueryAntiPattern,
)

TABLE_PATTERN = r"`?([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)`?"
SQL_FENCE_PATTERN = r"```sql\s*([\s\S]+?)\s*```"

_CATALOG_LOCK = threading.Lock()
_CATALOG_CACHE: dict[str, tuple[float, str]] = {}
_CATALOG_TTL = 300  # 5 minutes

SEVERITY_PENALTY = {
    "low": 5,
    "medium": 15,
    "high": 30,
    "critical": 50,
}

STRUCTURAL_PENALTY = {
    "select_star": 30,
    "cross_join": 30,
    "order_without_limit": 15,
    "order_by_rand": 20,
    "distinct": 10,
    "union_without_all": 10,
}


def _intelligence_report_to_text(report) -> str:
    """Converte IntelligenceReport estruturado em texto para prompts LLM."""
    if report is None:
        return "(não disponível)"
    parts: list[str] = []
    if report.table_alternatives:
        parts.append("Alternativas de tabela:\n" + "\n".join(f"- {x}" for x in report.table_alternatives))
    if report.partition_opportunities:
        parts.append("Oportunidades de partição:\n" + "\n".join(f"- {x}" for x in report.partition_opportunities))
    if report.clustering_opportunities:
        parts.append("Oportunidades de clustering:\n" + "\n".join(f"- {x}" for x in report.clustering_opportunities))
    if report.dataset_insights:
        parts.append("Insights do dataset:\n" + "\n".join(f"- {x}" for x in report.dataset_insights))
    if report.summary:
        parts.append(f"Resumo: {report.summary}")
    return "\n\n".join(parts) if parts else "Nenhuma oportunidade identificada."


def parse_query(state: AgentState) -> dict:
    query_upper = state.original_query.upper()

    has_star = bool(re.search(r"\bSELECT\s+\*", query_upper))
    has_order_without_limit = bool(
        re.search(r"ORDER\s+BY", query_upper)
        and not re.search(r"LIMIT\s+\d+", query_upper)
    )
    has_cross_join = bool(re.search(r"CROSS\s+JOIN", query_upper))
    has_order_by_rand = bool(re.search(r"ORDER\s+BY\s+RAND\s*\(", query_upper))
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
        "has_order_by_rand": has_order_by_rand,
        "has_distinct": has_distinct,
        "has_union_without_all": has_union_without_all,
        "join_count": join_count,
        "subquery_count": subquery_count,
        "cte_count": cte_count,
        "tables": tables,
    }

    return {"query_structure": structure}


def fetch_query_schema(state: AgentState) -> dict:
    """Busca schema das tabelas referenciadas na query."""
    return {"query_schema": _safe_get_schema_context(state)}


def fetch_dataset_catalog(state: AgentState) -> dict:
    """Busca catálogo completo de todas as tabelas do dataset + memória cross-sessão."""
    dataset_hint = state.dataset_hint
    if not dataset_hint:
        tables = state.query_structure.get("tables", [])
        if not tables:
            return {
                "dataset_catalog": "(catálogo indisponível — dataset não identificado)",
                "dataset_memory": "",
            }
        parts = tables[0].split(".")
        if len(parts) < 2:
            return {
                "dataset_catalog": "(catálogo indisponível — formato de tabela inválido)",
                "dataset_memory": "",
            }
        dataset_hint = f"{parts[0]}.{parts[1]}"

    # Load cross-session memory for this project/dataset
    memory_key = f"{state.project_id}:{dataset_hint}"
    dataset_memory = get_dataset_memory(memory_key)

    # Thread-safe TTL cache for catalog
    with _CATALOG_LOCK:
        cached = _CATALOG_CACHE.get(memory_key)
    if cached:
        ts, catalog_text = cached
        if time.time() - ts < _CATALOG_TTL:
            return {"dataset_catalog": catalog_text, "dataset_memory": dataset_memory}

    try:
        result = get_dataset_tables_schema(
            project_id=state.project_id,
            dataset_hint=dataset_hint,
            max_tables=30,
            max_columns=20,
        )
        tables_list = result.get("tables", [])
        if not tables_list:
            return {
                "dataset_catalog": "(catálogo vazio — nenhuma tabela encontrada no dataset)",
                "dataset_memory": dataset_memory,
            }

        lines = [f"Dataset: {result['dataset_ref']} ({len(tables_list)} tabelas)"]
        for t in tables_list:
            partition = f" [particionada: {t['partition_field']}]" if t.get("partition_field") else ""
            clustering = f" [clustering: {', '.join(t['clustering_fields'])}]" if t.get("clustering_fields") else ""
            cols = ", ".join(c["name"] for c in t.get("columns", [])[:10])
            lines.append(f"- {t['table_id']}{partition}{clustering}: {cols}")

        catalog_text = "\n".join(lines)
        with _CATALOG_LOCK:
            _CATALOG_CACHE[memory_key] = (time.time(), catalog_text)
        return {"dataset_catalog": catalog_text, "dataset_memory": dataset_memory}
    except Exception as exc:
        return {
            "dataset_catalog": f"(catálogo indisponível: {exc})",
            "dataset_memory": dataset_memory,
        }


def dry_run_baseline(state: AgentState) -> dict:
    """Dry-run da query original para custo baseline."""
    try:
        result = dry_run_query(state.original_query, state.project_id)
        return {"dry_run_original": result}
    except Exception as exc:
        from src.shared.tools.schemas import DryRunResult
        return {
            "dry_run_original": DryRunResult(
                bytes_processed=0,
                bytes_billed=0,
                estimated_cost_usd=0.0,
                error=str(exc),
            )
        }


def _is_simple_query(state: AgentState) -> bool:
    """Query simples: única tabela, sem JOINs, subqueries ou CTEs, E sem antipadrões estruturais.

    Não usa threshold de palavras — uma query curta com SELECT * ainda precisa de análise.
    """
    structure = state.query_structure
    return (
        len(structure.get("tables", [])) <= 1
        and structure.get("join_count", 0) == 0
        and structure.get("subquery_count", 0) == 0
        and structure.get("cte_count", 0) == 0
        and not structure.get("has_star")
        and not structure.get("has_cross_join")
    )


def enrich_with_intelligence(state: AgentState, llm: BaseChatModel) -> dict:
    """LLM com structured output analisa schema + catálogo + custo + memória do dataset."""
    if not state.dataset_catalog or "(indisponível" in state.dataset_catalog:
        return {"intelligence_report": None}

    cost_context = _build_cost_context(state.dry_run_original)
    memory_section = ""
    if state.dataset_memory:
        memory_section = f"\n\n## Histórico de padrões deste dataset (análises anteriores):\n{state.dataset_memory}"

    system_prompt = """\
Você é um arquiteto de dados BigQuery especialista em otimização de custos GCP.

Analise a query, o schema, o catálogo do dataset, o custo baseline e o histórico de padrões.
Retorne uma análise ESTRUTURADA com as seguintes seções:

- table_alternatives: tabelas particionadas, clusterizadas ou views materializadas no catálogo \
que poderiam substituir as usadas com menor custo (lista de strings, vazia se não houver)
- partition_opportunities: filtros de partição disponíveis não aplicados na query (lista de strings)
- clustering_opportunities: campos de clustering ignorados que poderiam reduzir bytes (lista de strings)
- dataset_insights: insights sobre estrutura/padrões do dataset relevantes para otimização (lista de strings)
- summary: parágrafo curto (2-3 frases) consolidando as oportunidades mais relevantes

Se não houver oportunidades em uma seção, retorne lista vazia. Seja específico e direto.
"""

    user_prompt = f"""## Query:
```sql
{state.original_query}
```

## Schema das tabelas na query:
{state.query_schema or "(indisponível)"}

## Catálogo completo do dataset:
{state.dataset_catalog or "(indisponível)"}

## Custo baseline (dry-run):
{cost_context}{memory_section}
"""

    try:
        structured_llm = llm.with_structured_output(IntelligenceReport)
        report: IntelligenceReport = invoke_with_retry(
            structured_llm,
            [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)],
        )
        if report is None:
            report = IntelligenceReport()
    except Exception as exc:
        report = IntelligenceReport(summary=f"(enriquecimento indisponível: {exc})")

    return {"intelligence_report": report}


# ---------------------------------------------------------------------------
# Nó legado mantido para compatibilidade com grafos externos
# ---------------------------------------------------------------------------

def dry_run_estimate(state: AgentState) -> dict:
    return dry_run_baseline(state)


def detect_antipatterns(state: AgentState, llm: BaseChatModel) -> dict:
    """Híbrido: regras determinísticas + LLM com structured output."""
    structure = state.query_structure
    dry = state.dry_run_original
    cost_context = _build_cost_context(dry)

    # Use typed fields — not the deprecated schema_context frankenstein
    schema_section = state.query_schema or _safe_get_schema_context(state)
    intelligence_section = _intelligence_report_to_text(state.intelligence_report)

    antipatterns_list = "\n".join(f"- {pattern}" for pattern in BQ_ANTIPATTERNS)

    user_prompt = f"""
QUERY:
{state.original_query}

ANÁLISE ESTRUTURAL:
{json.dumps(structure, indent=2, ensure_ascii=False)}

CUSTO BASELINE:
{cost_context}

SCHEMA DAS TABELAS:
{schema_section}

INTELIGÊNCIA DE DATASET (alternativas, partições disponíveis, oportunidades):
{intelligence_section}

ANTIPADRÕES POSSÍVEIS:
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
    bytes_warn = int(get_runtime_config("BYTES_WARNING_THRESHOLD", str(10 * 1024**3)))
    if dry and not dry.error and dry.bytes_processed > bytes_warn:
        needs_optimization = True

    return {
        "antipatterns": antipatterns,
        "needs_optimization": needs_optimization,
    }


def analyze_patterns(state: AgentState, llm: BaseChatModel) -> dict:
    """Legado — mantido para compatibilidade com grafos externos."""
    return detect_antipatterns(state, llm)


def await_human_approval(state: AgentState) -> dict:
    """Pausa o pipeline para aprovação humana. Sem antipadrões: avança automaticamente."""
    if not state.needs_optimization:
        return {"human_decision": "skip"}

    payload: dict[str, Any] = {
        "message": "Antipadrões identificados. Deseja prosseguir com a otimização?",
        "antipatterns": [
            {
                "pattern": ap.pattern,
                "severity": ap.severity,
                "description": ap.description,
                "suggestion": ap.suggestion,
            }
            for ap in state.antipatterns
        ],
    }

    dry = state.dry_run_original
    if dry and not dry.error:
        payload["bytes_processed"] = dry.bytes_processed
        payload["estimated_cost_usd"] = dry.estimated_cost_usd

    decision: str = interrupt(payload)
    return {"human_decision": decision}


def optimize_query(state: AgentState, llm: BaseChatModel) -> dict:
    antipatterns_text = _build_antipatterns_text(state.antipatterns)
    cost_info = _build_optimization_cost_context(state.dry_run_original)

    # Use typed fields instead of the deprecated schema_context
    schema_available = bool(state.query_schema and "(indisponível)" not in state.query_schema)
    schema_section = state.query_schema if schema_available else "(schema não disponível)"
    intel_text = _intelligence_report_to_text(state.intelligence_report)
    if intel_text and intel_text != "(não disponível)":
        schema_section += f"\n\nINTELIGÊNCIA DE CONTEXTO:\n{intel_text}"

    is_complex_analytical = _is_complex_analytical_query(state)

    # Build iteration-aware feedback
    prior_feedback = list(state.optimization_feedback or [])
    is_retry = state.iteration > 0
    if is_retry and prior_feedback:
        feedback_text = "TENTATIVA ANTERIOR FALHOU:\n" + "\n".join(f"- {f}" for f in prior_feedback)
        feedback_text += "\n\nTente uma ABORDAGEM DIFERENTE — considere reescrever a estrutura da query em vez de apenas ajustar filtros."
    else:
        feedback_text = "(primeira tentativa de otimização)"

    schema_instruction = (
        "Projete apenas as colunas necessárias listadas no schema acima."
        if schema_available
        else "O schema não está disponível — mantenha SELECT * se presente; NUNCA substitua por colunas que você não viu explicitamente."
    )

    gold_layer_note = ""
    if is_complex_analytical:
        gold_layer_note = """
ATENÇÃO — Query analítica complexa (possível camada Gold/Silver):
- Foque APENAS em otimizações de infraestrutura (partições, clustering, UNION ALL)
- NUNCA remova, renomeie ou reescreva colunas calculadas ou aliases de KPI
- NUNCA simplifique CASE WHEN ou lógica de negócio
"""

    system_prompt = f"""Você é um especialista em BigQuery e SQL analítico para Power BI.
Otimize a query para reduzir bytes processados e slots consumidos no BigQuery.

REGRAS ABSOLUTAS — nunca viole nenhuma delas:
1. Preserve EXATAMENTE todas as colunas calculadas, aliases e métricas de KPI
2. Preserve TODAS as regras de negócio: CASE WHEN, IF, COALESCE, IIF, NULLIF
3. Preserve TODOS os JOINs necessários
4. Preserve TODA a lógica de datas, períodos e granularidade temporal
5. Preserve TODOS os filtros WHERE e HAVING existentes na query original
6. {schema_instruction}
7. NUNCA adicione cláusulas WHERE, filtros ou condições que NÃO estavam na query original — você pode otimizar estruturas (UNION ALL, partições, clustering) mas JAMAIS restringir os dados retornados
8. O conjunto de linhas retornado DEVE ser idêntico ao original
9. NUNCA adicione comentários no SQL final
10. Responda APENAS com o SQL otimizado
11. Evite ORDER BY RAND(); substitua por TABLESAMPLE ou remova se não for essencial
12. Implicit cross joins (FROM a, b) devem ser convertidos em INNER JOIN explícito APENAS quando a relação de chave for 100% clara no schema; caso contrário, mantenha o FROM original
{gold_layer_note}"""

    user_prompt = f"""## Query original:
```sql
{state.original_query}
```

Anti-padrões identificados para corrigir:

{antipatterns_text}

Contexto de custo (BigQuery dry-run):

{cost_info}

Schema e contexto disponíveis:

{schema_section}

{feedback_text}
"""

    try:
        response = llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ])

        raw = _extract_message_content(response)
        optimized = _sanitize_optimized_sql(_extract_sql_from_response(raw))

        if not optimized:
            return {
                "optimized_query": state.original_query,
                "iteration": state.iteration + 1,
                "optimization_feedback": prior_feedback + ["LLM não retornou SQL válida; mantendo original."],
            }

        return {
            "optimized_query": optimized,
            "iteration": state.iteration + 1,
            "optimization_feedback": [],  # reset on success
        }
    except Exception as exc:
        return {
            "optimized_query": state.original_query,
            "iteration": state.iteration + 1,
            "optimization_feedback": prior_feedback + [f"Falha ao otimizar SQL via LLM: {exc}"],
        }


def validate_optimized(state: AgentState) -> dict:
    if not state.optimized_query:
        return {}

    # Idempotency check: se o LLM retornou a query idêntica, não há ganho — vai direto ao relatório
    original_stripped = re.sub(r"\s+", " ", state.original_query.strip().lower())
    optimized_stripped = re.sub(r"\s+", " ", state.optimized_query.strip().lower())
    if original_stripped == optimized_stripped:
        return {
            "needs_optimization": False,
            "optimization_feedback": list(state.optimization_feedback) + [
                "Query otimizada é idêntica à original — nenhuma melhoria encontrada."
            ],
        }

    try:
        result = dry_run_query(state.optimized_query, state.project_id)
    except Exception as exc:
        from src.shared.tools.schemas import DryRunResult
        return {
            "dry_run_optimized": DryRunResult(
                bytes_processed=0, bytes_billed=0, estimated_cost_usd=0.0, error=str(exc)
            ),
            "needs_optimization": True,
            "optimization_feedback": list(state.optimization_feedback) + [f"Dry-run falhou: {exc}"],
        }

    feedback: list[str] = list(state.optimization_feedback)  # accumulate, don't reset
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

        if optimized_structure["has_order_by_rand"]:
            needs_optimization = True
            feedback.append("A query otimizada manteve ORDER BY RAND().")

        if state.query_structure.get("has_distinct") and optimized_structure["has_distinct"]:
            feedback.append("A query otimizada ainda usa DISTINCT; valide se a deduplicação é necessária.")

        if (
            state.query_structure.get("has_union_without_all")
            and optimized_structure["has_union_without_all"]
        ):
            feedback.append("A query otimizada manteve UNION sem ALL.")

        dry_orig = state.dry_run_original
        if dry_orig and not dry_orig.error and dry_orig.bytes_processed > 0:
            if result.bytes_processed >= dry_orig.bytes_processed:
                needs_optimization = True
                feedback.append("A query otimizada não reduziu bytes processados em relação à original.")
            else:
                savings_pct = (
                    (dry_orig.bytes_processed - result.bytes_processed)
                    / dry_orig.bytes_processed * 100
                )
                if savings_pct < 5:
                    needs_optimization = True
                    feedback.append(
                        f"Economia de apenas {savings_pct:.1f}% — abaixo de 5%. Tente reduzir full scan."
                    )

    return {
        "dry_run_optimized": result,
        "needs_optimization": needs_optimization,
        "optimization_feedback": feedback,
    }


def validate_data_existence(state: AgentState) -> dict:
    """Detecta mudanças semânticas na query otimizada (WHERE adicionado pelo optimizer).

    Verifica APENAS via análise léxica — sem execução real no BigQuery.
    O custo de execução para checar existência de dados é maior do que o benefício,
    dado que o prompt já proíbe explicitamente adicionar cláusulas WHERE.
    """
    if not state.optimized_query:
        return {}

    warnings: list[str] = []

    # — Verificação semântica: WHERE adicionado pelo optimizer ——————————————
    original_has_where = bool(re.search(r"\bWHERE\b", state.original_query, re.IGNORECASE))
    optimized_has_where = bool(re.search(r"\bWHERE\b", state.optimized_query, re.IGNORECASE))

    if optimized_has_where and not original_has_where:
        warnings.append(
            "⚠ A query otimizada adicionou cláusula WHERE que NÃO existia na query original. "
            "Isso pode restringir os dados retornados. "
            "Revise a query otimizada antes de usar."
        )

    if original_has_where and optimized_has_where:
        orig_conds = _extract_where_conditions(state.original_query)
        opt_conds = _extract_where_conditions(state.optimized_query)
        new_conds = opt_conds - orig_conds
        if new_conds:
            warnings.append(
                f"⚠ A query otimizada adicionou {len(new_conds)} condição(ões) de filtro "
                f"além das originais: {', '.join(sorted(new_conds)[:3])}. "
                "Confirme que essas condições não alteram o conjunto de resultados esperado."
            )

    return {"data_existence_warning": "\n\n".join(warnings) if warnings else None}


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

    summary = _generate_summary(state=state, score=score, grade=grade, savings_pct=savings_pct, dry_orig=dry_orig)

    recommendations = [ap.suggestion for ap in state.antipatterns]
    if not recommendations:
        recommendations = ["Query está seguindo boas práticas do BigQuery."]

    applied_optimizations = _build_applied_optimizations(state)

    # optimization_status: por que a otimização foi/não foi aplicada
    if state.human_decision == "skip":
        opt_status = "skipped_by_user"
    elif not state.needs_optimization and not state.antipatterns:
        opt_status = "skipped_no_antipatterns"
    elif state.optimized_query and state.optimized_query != state.original_query:
        opt_status = "approved"
    else:
        opt_status = "failed"

    # data_quality: indica se os dados de custo estão completos
    if dry_orig and not dry_orig.error and dry_opt and not dry_opt.error:
        dq = "full"
    elif dry_orig and not dry_orig.error:
        dq = "partial"
    else:
        dq = "no_cost_data"

    intelligence_summary = _intelligence_report_to_text(state.intelligence_report)
    if intelligence_summary == "(não disponível)" or intelligence_summary == "Nenhuma oportunidade identificada.":
        intelligence_summary = None

    report = OptimizationReport(
        efficiency_score=score,
        grade=grade,
        summary=summary,
        antipatterns_found=state.antipatterns,
        optimized_query=state.optimized_query,
        original_query=state.original_query,
        bytes_saved=bytes_saved,
        cost_saved_usd=cost_saved,
        savings_pct=savings_pct,
        recommendations=recommendations,
        power_bi_tips=power_bi_tips,
        applied_optimizations=applied_optimizations,
        intelligence_summary=intelligence_summary,
        data_existence_warning=state.data_existence_warning,
        optimization_status=opt_status,
        data_quality=dq,
    )

    # Persist cross-session memory: antipatterns + intelligence context for this dataset
    _save_analysis_to_memory(state)

    return {"report": report}


def score_and_report(state: AgentState, llm: BaseChatModel) -> dict:
    """Alias de generate_report com nomenclatura alinhada à nova topologia."""
    return generate_report(state, llm)


# ---------------------------------------------------------------------------
# Memory helpers
# ---------------------------------------------------------------------------

def _save_analysis_to_memory(state: AgentState) -> None:
    """Persiste antipadrões desta análise na memória cross-sessão do dataset."""
    try:
        tables = state.query_structure.get("tables", [])
        if not tables:
            return
        parts = tables[0].split(".")
        if len(parts) < 2:
            return
        dataset_hint = state.dataset_hint or f"{parts[0]}.{parts[1]}"
        memory_key = f"{state.project_id}:{dataset_hint}"

        entries = [
            {
                "pattern": ap.pattern,
                "severity": ap.severity,
                "suggestion": ap.suggestion or "",
            }
            for ap in state.antipatterns
        ]

        if entries:
            update_dataset_memory(memory_key, entries)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _extract_where_conditions(sql: str) -> set[str]:
    """Extrai termos de condição do WHERE para comparar entre original e otimizada."""
    match = re.search(
        r"\bWHERE\b(.+?)(?:\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|\bUNION\b|$)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return set()
    where_text = match.group(1).strip()
    # Split on AND/OR and extract individual condition tokens (column names, values)
    tokens = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_.]*\b", where_text)
    # Filter out SQL keywords
    sql_keywords = {"AND", "OR", "NOT", "IN", "IS", "NULL", "BETWEEN", "LIKE", "EXISTS", "AS"}
    return {t.upper() for t in tokens if t.upper() not in sql_keywords and len(t) > 2}


def _safe_get_schema_context(state: AgentState) -> str:
    try:
        return get_schemas_for_query(state.original_query, state.project_id)
    except Exception:
        return "(detalhes de schema não disponíveis)"


def _build_cost_context(dry: Any) -> str:
    if dry and not dry.error:
        return (
            f"Bytes Processados: {format_bytes(dry.bytes_processed)} | "
            f"Custo Estimado: USD {dry.estimated_cost_usd:.4f}"
        )
    if dry and dry.error:
        return f"Erro técnico no dry-run: {dry.error}"
    return "Estimativa indisponível (dry-run não executado ou falhou)"


def _extract_message_content(response: Any) -> str:
    if hasattr(response, "content"):
        return str(response.content).strip()
    return str(response).strip()


def _strip_code_fences(raw: str) -> str:
    cleaned = raw.strip()
    cleaned = re.sub(r"^```(?:json|sql)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _remove_sql_comments(sql: str) -> str:
    without_block = re.sub(r"/\*[\s\S]*?\*/", "", sql)
    without_line = re.sub(r"--[^\n\r]*", "", without_block)
    return without_line


def _sanitize_optimized_sql(sql: str) -> str:
    no_comments = _remove_sql_comments(sql)
    lines = [line.rstrip() for line in no_comments.splitlines()]
    compact: list[str] = []
    previous_blank = False
    for line in lines:
        is_blank = not line.strip()
        if is_blank and previous_blank:
            continue
        compact.append(line)
        previous_blank = is_blank
    return "\n".join(compact).strip()


def _extract_sql_from_response(raw: str) -> str:
    sql_match = re.search(SQL_FENCE_PATTERN, raw, re.IGNORECASE)
    if sql_match:
        return sql_match.group(1).strip()
    return _strip_code_fences(raw)


def _detect_antipatterns_with_llm(
    llm: BaseChatModel,
    system_prompt: str,
    user_prompt: str,
) -> list[QueryAntiPattern]:
    try:
        structured_llm = llm.with_structured_output(AntipatternList)
        result: AntipatternList = invoke_with_retry(
            structured_llm,
            [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)],
        )
        return result.antipatterns if result else []
    except Exception:
        return []


def _build_forced_antipatterns(structure: dict[str, Any]) -> list[QueryAntiPattern]:
    forced: list[QueryAntiPattern] = []

    if structure.get("has_star"):
        forced.append(QueryAntiPattern(
            pattern="SELECT *",
            description="Uso de SELECT * detectado via análise estrutural",
            severity="HIGH",
            suggestion="Projete apenas as colunas necessárias para reduzir leitura de dados",
        ))

    if structure.get("has_order_without_limit"):
        forced.append(QueryAntiPattern(
            pattern="ORDER BY sem LIMIT",
            description="ORDER BY global sem LIMIT detectado",
            severity="HIGH",
            suggestion="Adicione LIMIT ou remova ORDER BY para evitar ordenação completa",
        ))

    if structure.get("has_cross_join"):
        forced.append(QueryAntiPattern(
            pattern="CROSS JOIN sem filtro",
            description="CROSS JOIN detectado via análise estrutural",
            severity="HIGH",
            suggestion="Evite CROSS JOIN sem filtro explícito",
        ))

    if structure.get("has_order_by_rand"):
        forced.append(QueryAntiPattern(
            pattern="ORDER BY RAND()",
            description="Ordenação aleatória global detectada",
            severity="HIGH",
            suggestion="Evite ORDER BY RAND(); prefira LIMIT simples ou TABLESAMPLE",
        ))

    if structure.get("has_distinct"):
        forced.append(QueryAntiPattern(
            pattern="DISTINCT sem necessidade real",
            description="Uso de DISTINCT detectado via análise estrutural",
            severity="MEDIUM",
            suggestion="Valide se DISTINCT é realmente necessário",
        ))

    if structure.get("has_union_without_all"):
        forced.append(QueryAntiPattern(
            pattern="UNION sem ALL",
            description="UNION detectado sem ALL via análise estrutural",
            severity="MEDIUM",
            suggestion="Use UNION ALL quando não for necessário remover duplicidades",
        ))

    return forced


# Mapeamento de padrões canônicos: detecta o mesmo problema com diferentes nomes
_ANTIPATTERN_CANONICAL: dict[str, str] = {
    "select *": "select_star",
    "select*": "select_star",
    "uso de select *": "select_star",
    "cross join": "cross_join",
    "cross join sem filtro": "cross_join",
    "produto cartesiano": "cross_join",
    "order by sem limit": "order_without_limit",
    "order by global sem limit": "order_without_limit",
    "order by rand": "order_by_rand",
    "order by rand()": "order_by_rand",
    "ordenação aleatória": "order_by_rand",
    "distinct": "distinct",
    "distinct desnecessário": "distinct",
    "distinct sem necessidade real": "distinct",
    "union sem all": "union_without_all",
}


def _antipattern_group(pattern: str) -> str:
    """Retorna grupo canônico do padrão para deduplicação semântica."""
    key = pattern.strip().lower()
    return _ANTIPATTERN_CANONICAL.get(key, key)


def _merge_antipatterns(
    detected: list[QueryAntiPattern],
    forced: list[QueryAntiPattern],
) -> list[QueryAntiPattern]:
    """Mescla antipadrões LLM + regras com deduplicação semântica por grupo canônico."""
    merged = list(detected)
    existing_groups = {_antipattern_group(ap.pattern) for ap in merged}
    for forced_item in forced:
        group = _antipattern_group(forced_item.pattern)
        if group not in existing_groups:
            merged.append(forced_item)
            existing_groups.add(group)
    return merged


def _build_antipatterns_text(antipatterns: list[QueryAntiPattern]) -> str:
    if not antipatterns:
        return "Nenhum anti-padrão identificado."
    return "\n".join(
        f"[{ap.severity.upper()}] {ap.pattern}: {ap.description}"
        for ap in antipatterns
    )


def _build_optimization_cost_context(dry: Any) -> str:
    if dry and not dry.error:
        return (
            f"A query original processa {format_bytes(dry.bytes_processed)} "
            f"(USD {dry.estimated_cost_usd:.4f}). "
            "Aplique otimizações para reduzir ao máximo esses valores."
        )
    if dry and dry.error:
        return f"Dry-run original apresentou erro: {dry.error}"
    return "Custo original não disponível."


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
        "has_order_by_rand": bool(re.search(r"ORDER\s+BY\s+RAND\s*\(", query_upper)),
        "has_distinct": bool(re.search(r"\bDISTINCT\b", query_upper)),
        "has_union_without_all": bool(re.search(r"\bUNION\b(?!\s+ALL)", query_upper)),
    }


def _structural_flags_to_keys(structure: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    if structure.get("has_star"):
        keys.add("select_star")
    if structure.get("has_cross_join"):
        keys.add("cross_join")
    if structure.get("has_order_without_limit"):
        keys.add("order_without_limit")
    if structure.get("has_order_by_rand"):
        keys.add("order_by_rand")
    if structure.get("has_distinct"):
        keys.add("distinct")
    if structure.get("has_union_without_all"):
        keys.add("union_without_all")
    return keys


def _extract_limit_values(sql: str) -> list[int]:
    values: list[int] = []
    for match in re.finditer(r"\bLIMIT\s+(\d+)\b", sql, flags=re.IGNORECASE):
        try:
            values.append(int(match.group(1)))
        except Exception:
            continue
    return values


def _build_applied_optimizations(state: AgentState) -> list[str]:
    items: list[str] = []
    original_query = state.original_query or ""
    optimized_query = state.optimized_query or ""

    if original_query and optimized_query:
        original_structure = _inspect_query_structure(original_query)
        optimized_structure = _inspect_query_structure(optimized_query)

        if original_structure["has_star"] and not optimized_structure["has_star"]:
            items.append(
                "Melhoria aplicada: remoção de SELECT * com projeção de colunas necessárias, reduzindo leitura de dados e consumo de slots."
            )
        if original_structure["has_cross_join"] and not optimized_structure["has_cross_join"]:
            items.append(
                "Melhoria aplicada: eliminação de CROSS JOIN sem filtro, evitando explosão combinatória e alto uso de slots."
            )
        if original_structure["has_order_without_limit"] and not optimized_structure["has_order_without_limit"]:
            items.append(
                "Melhoria aplicada: ajuste de ORDER BY sem LIMIT para reduzir ordenação global desnecessária."
            )

    if state.antipatterns:
        for ap in state.antipatterns:
            suggestion = (ap.suggestion or "").strip()
            if suggestion:
                items.append(f"Critério considerado: {ap.pattern}. Ação tomada: {suggestion}")

    if optimized_query and "--" not in optimized_query and "/*" not in optimized_query:
        items.append("SQL final higienizada: comentários removidos para entrega limpa.")

    limits = _extract_limit_values(optimized_query)
    if limits:
        max_limit = max(limits)
        if max_limit >= 100000:
            items.append(f"Critério de custo/slot: LIMIT extremo detectado ({max_limit}).")
        else:
            items.append("Critério de custo/slot: LIMIT em faixa controlada.")

    if state.dry_run_original and state.dry_run_optimized:
        dry_orig = state.dry_run_original
        dry_opt = state.dry_run_optimized
        if not dry_orig.error and not dry_opt.error and dry_orig.bytes_processed > 0:
            bytes_saved = max(0, dry_orig.bytes_processed - dry_opt.bytes_processed)
            pct = bytes_saved / dry_orig.bytes_processed * 100
            cost_saved = max(0.0, dry_orig.estimated_cost_usd - dry_opt.estimated_cost_usd)
            items.append(
                f"Impacto estimado (dry-run): {pct:.1f}% de redução de bytes "
                f"({format_bytes(bytes_saved)} a menos), economia aprox. USD {cost_saved:.4f}."
            )

    if not items:
        items.append(
            "Critérios avaliados: redução de bytes processados, diminuição de operações custosas, preservação de KPIs."
        )

    dedup: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = item.strip().lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            dedup.append(item.strip())

    return dedup or ["Nenhuma otimização relevante foi necessária para esta query."]


def _generate_summary(
    state: AgentState,
    score: int,
    grade: str,
    savings_pct: float | None,
    dry_orig: Any,
) -> str:
    bytes_text = format_bytes(dry_orig.bytes_processed) if dry_orig and not dry_orig.error else "N/A"
    savings_text = f"{savings_pct}%" if savings_pct is not None else "não calculada"
    return (
        f"Query analisada com score {score}/100 ({grade}). "
        f"Foram identificados {len(state.antipatterns)} anti-padrão(s), "
        f"com economia estimada de {savings_text} sobre {bytes_text} processados na query original."
    )


def _calculate_score(state: AgentState) -> int:
    score = 100

    dry_orig = state.dry_run_original
    dry_opt = state.dry_run_optimized

    # Compute savings percentage for proportional multiplier (#8)
    savings_ratio = 0.0
    improved_bytes = False
    if (
        dry_orig and dry_opt
        and not dry_orig.error and not dry_opt.error
        and dry_orig.bytes_processed > 0
        and dry_opt.bytes_processed < dry_orig.bytes_processed
    ):
        improved_bytes = True
        savings_ratio = (dry_orig.bytes_processed - dry_opt.bytes_processed) / dry_orig.bytes_processed

    # Proportional multiplier: more savings → stronger penalty reduction
    if improved_bytes:
        # 0% savings → 0.85, 50% savings → 0.475, 100% savings → 0.10 (floor)
        penalty_multiplier = max(0.10, 0.85 - 0.75 * savings_ratio)
    elif state.optimized_query:
        penalty_multiplier = 0.60
    else:
        penalty_multiplier = 1.0

    for antipattern in state.antipatterns:
        severity = (antipattern.severity or "").strip().lower()
        score -= int(SEVERITY_PENALTY.get(severity, 10) * penalty_multiplier)

    final_structure = (
        _inspect_query_structure(state.optimized_query)
        if state.optimized_query
        else state.query_structure
    )
    final_structural_keys = _structural_flags_to_keys(final_structure)
    original_structural_keys = _structural_flags_to_keys(state.query_structure)

    # Only penalize structural issues NEW in the final query (not already in antipatterns)
    new_structural_keys = final_structural_keys - original_structural_keys
    for key in new_structural_keys:
        score -= STRUCTURAL_PENALTY.get(key, 0)

    dry = dry_opt if (dry_opt and not dry_opt.error) else dry_orig
    if dry and not dry.error:
        bytes_crit = int(get_runtime_config("BYTES_CRITICAL_THRESHOLD", str(100 * 1024**3)))
        bytes_warn2 = int(get_runtime_config("BYTES_WARNING_THRESHOLD", str(10 * 1024**3)))
        if dry.bytes_processed > bytes_crit:
            score -= 15
        elif dry.bytes_processed > bytes_warn2:
            score -= 8

    removed_structural = original_structural_keys - final_structural_keys
    if removed_structural:
        score += min(20, len(removed_structural) * 5)

    if improved_bytes:
        savings_pct = savings_ratio * 100
        if savings_pct >= 70:
            score += 30
        elif savings_pct >= 50:
            score += 22
        elif savings_pct >= 30:
            score += 16
        elif savings_pct >= 15:
            score += 10
        elif savings_pct >= 5:
            score += 5

        major_remaining = {"select_star", "cross_join", "order_without_limit"} & final_structural_keys
        if savings_pct >= 50 and not major_remaining:
            score = max(score, 90)
        elif savings_pct >= 30 and not major_remaining:
            score = max(score, 82)

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
            "No desenvolvimento do dashboard, modele consultas com colunas estritamente necessárias (evite SELECT *) para reduzir tempo de refresh e consumo de capacidade."
        )

    if structure.get("has_order_without_limit"):
        tips.append(
            "Evite ORDER BY sem necessidade no dataset do dashboard; priorize ordenação no visual e preserve a consulta base enxuta."
        )

    if structure.get("join_count", 0) >= 3:
        tips.append(
            "Com muitos JOINs na query base, considere criar uma view materializada ou tabela desnormalizada no BigQuery para acelerar o refresh do dataset Power BI."
        )

    if structure.get("cte_count", 0) >= 2:
        tips.append(
            "Queries com múltiplas CTEs podem se beneficiar de tabelas intermediárias persistidas no BigQuery para reduzir latência nos refreshes agendados do Power BI."
        )

    # Surface first actionable insight from intelligence_report
    report = state.intelligence_report
    if report:
        if report.partition_opportunities:
            tips.append(
                f"Contexto do dataset: {report.partition_opportunities[0]} — aplique este filtro de partição também nos datasets do Power BI para refresh incremental mais eficiente."
            )
        elif report.table_alternatives:
            tips.append(
                f"Contexto do dataset: {report.table_alternatives[0]} — considere essa alternativa ao construir o dataset do Power BI."
            )
        elif report.dataset_insights:
            first = report.dataset_insights[0]
            tips.append(
                f"Insight do dataset: {first} — considere essa perspectiva ao modelar hierarquias e relacionamentos no Power BI."
            )

    tips.extend([
        "Adote modelo estrela (fato + dimensões) com relacionamentos simples para melhorar desempenho e clareza das medidas DAX.",
        "Crie medidas reutilizáveis e padronizadas (nomenclatura e formato) para garantir consistência de KPI entre páginas e dashboards.",
        "Use refresh incremental e filtros por data/partição no backend para evitar full scan em atualizações.",
        "Otimize UX: limite visuais por página, mantenha hierarquia visual clara e use navegação orientada a perguntas de negócio.",
        "Implemente governança: RLS, dicionário de métricas e validação de qualidade de dados antes da publicação.",
    ])

    return tips
