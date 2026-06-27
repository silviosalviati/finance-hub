from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.types import interrupt

from src.agents.query_build.prompts import (
	QUERY_BUILD_REVIEWER_PROMPT,
	QUERY_BUILD_SYSTEM_PROMPT,
)
from src.agents.query_build.state import QueryBuildState
from src.shared.config import get_runtime_config
from src.shared.guardrails import rbac
from src.shared.guardrails.audit import record as record_audit_entry
from src.shared.guardrails.sql_safety import assert_select_only
from src.shared.guardrails.temporal import get_date_block
from src.shared.tools.bigquery import (
	fetch_query_sample,
	dry_run_query,
	get_dataset_tables_schema,
)
from src.shared.tools.llm import invoke_with_retry

_DEFAULT_QUERY_BUDGET_BYTES = 5 * 1024 ** 3

SQL_FENCE_PATTERN = r"```sql\s*([\s\S]+?)\s*```"
TABLE_REF_PATTERN = r"`?([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)`?"
TABLE_ALIAS_PATTERN = (
	r"\b(?:FROM|JOIN)\s+`?([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)`?"
	r"(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?"
)
QUALIFIED_COLUMN_PATTERN = r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b"
NAMED_PARAM_PATTERN = r"(?<!@)@([A-Za-z_][A-Za-z0-9_]*)"
MUSTACHE_PARAM_PATTERN = r"\{\{\s*([^{}\s]+)\s*\}\}"
DOLLAR_PARAM_PATTERN = r"\$\{\s*([^{}\s]+)\s*\}"
INVALID_STRING_AGG_PATTERN = (
	r"\b(AVG|SUM|MIN|MAX)\s*\(\s*CAST\s*\(\s*(.*?)\s+AS\s+STRING\s*\)\s*\)"
)
SELECT_STAR_PATTERN = r"select\s+\*"
ORDER_BY_ORDINAL_PATTERN = r"order\s+by\s+\d+(?:\s*,\s*\d+)*\b"
INLINE_COMMENT_PATTERN = r"--[^\n]*|/\*.*?\*/"

_QUALITY_JUDGE_SYSTEM_PROMPT = """\
Você é um Revisor Sênior de SQL BigQuery. Avalie a query fornecida contra 2 critérios objetivos.

Responda SOMENTE em JSON válido, sem markdown, sem texto adicional.

FORMATO DE RESPOSTA:
{
  "single_scan_ok": true,
  "single_scan_reason": "Resumo objetivo (1 frase).",
  "fields_coherent_ok": true,
  "fields_coherent_reason": "Resumo objetivo (1 frase)."
}

CRITÉRIOS (seja conservador — marque false somente quando o problema for claro e objetivo):
- single_scan_ok: false APENAS se a query usa JOINs/CTEs/self-joins evitáveis com agregação condicional numa única leitura da tabela principal.
- fields_coherent_ok: false APENAS se os campos selecionados claramente não respondem à pergunta original do usuário (campo errado ou ausente para algo essencial pedido).
"""


def check_access(state: QueryBuildState) -> dict[str, Any]:
	"""RBAC antes de qualquer chamada de LLM — sem isso, gastaríamos uma
	geração de SQL inteira (custo de LLM) só pra descobrir depois que o
	usuário nem pode acessar o dataset pedido.
	"""
	if not state.dataset_hint:
		return {}

	allowed, reason = rbac.check_dataset(state.user, state.dataset_hint)
	if allowed:
		return {}

	return {
		"error": (
			f"Você não tem permissão para acessar o dataset '{state.dataset_hint}'. "
			"Escolha outro dataset ou solicite acesso."
		),
		"error_category": "rbac",
		"warnings": [f"Bloqueado por RBAC: {reason}"] if reason else [],
	}


def generate_sql_from_request(state: QueryBuildState, llm: BaseChatModel) -> dict[str, Any]:
	# Reentrada após erro recuperável (validate_sql/dry_run_generated) com
	# repair_attempts < 1 — ver _guard_repairable em graph.py.
	is_repair = bool(state.error)
	previous_error = state.error if is_repair else None
	repair_attempts = state.repair_attempts + 1 if is_repair else state.repair_attempts

	# Reentrada por decisao humana "melhorar" apos score < 80 (await_quality_approval).
	is_quality_retry = not is_repair and state.human_decision == "melhorar"
	quality_retry_count = (
		state.quality_retry_count + 1 if is_quality_retry else state.quality_retry_count
	)

	dataset_tables: list[str] = []
	dataset_table_columns: dict[str, list[str]] = {}
	dataset_table_meta: dict[str, dict[str, Any]] = {}
	dataset_context = ""
	dataset_warning: str | None = None

	if state.dataset_hint:
		try:
			schema = get_dataset_tables_schema(state.project_id, state.dataset_hint)
			dataset_tables = [item["full_name"] for item in schema.get("tables", [])]
			dataset_table_columns = _build_table_columns_map(schema.get("tables", []))
			dataset_table_meta = _build_table_meta_map(schema.get("tables", []))

			if dataset_tables:
				lines = [
					"Catalogo real de tabelas disponiveis no dataset (use somente estas tabelas):"
				]
				for item in schema.get("tables", []):
					cols_raw: list[dict] = item.get("columns") or []
					col_parts = [
						f"{c['name']} ({c.get('type', '?')})" for c in cols_raw
					] if cols_raw else ["(colunas nao carregadas)"]
					partition = item.get("partition_field") or ""
					clustering = ", ".join(item.get("clustering_fields") or [])

					meta_parts: list[str] = []
					if partition:
						meta_parts.append(f"particionada por: {partition}")
					if clustering:
						meta_parts.append(f"clusterizada por: {clustering}")
					meta_suffix = f" | {'; '.join(meta_parts)}" if meta_parts else ""

					lines.append(
						f"- {item['full_name']}{meta_suffix}"
						f"\n  colunas: {', '.join(col_parts)}"
					)
				dataset_context = "\n".join(lines)
			else:
				dataset_warning = (
					"Dataset informado nao retornou tabelas visiveis para o service account."
				)
		except Exception as exc:
			dataset_warning = f"Nao foi possivel carregar tabelas reais do dataset: {exc}"

	if is_repair:
		repair_block = (
			f"\nTENTATIVA ANTERIOR FALHOU: {previous_error}\n"
			"Corrija especificamente esse problema na nova versao da SQL.\n"
		)
	elif is_quality_retry:
		issues_text = "; ".join(state.quality_issues) or "qualidade insuficiente nos pilares obrigatorios"
		repair_block = (
			f"\nA VERSAO ANTERIOR TEVE NOTA DE QUALIDADE {state.quality_score}/100, abaixo do minimo aceitavel. "
			f"Problemas identificados: {issues_text}. "
			"Gere uma nova versao que corrija esses pontos especificamente, mantendo a mesma intencao de negocio.\n"
		)
	else:
		repair_block = ""

	user_prompt = f"""
Solicitacao do usuario:
{state.request_text}

Project ID:
{state.project_id}

Dataset hint (opcional):
{state.dataset_hint or '(nao informado)'}

{dataset_context or 'Catalogo de tabelas indisponivel para esta solicitacao.'}
{repair_block}
Regra obrigatoria:
- Use apenas tabelas reais listadas no catalogo acima.
- Nao invente nome de tabela.
- Valide tipo de colunas no schema fornecido antes de montar JOINs e filtros.
- Em colunas equivalentes com tipos diferentes (ex.: STRING vs INT64), use CAST explicito para compatibilizar.
"""

	system_prompt = QUERY_BUILD_SYSTEM_PROMPT.replace("__DATE_BLOCK__", get_date_block(date.today()))

	try:
		response = invoke_with_retry(
			llm,
			[
				SystemMessage(content=system_prompt),
				HumanMessage(content=user_prompt),
			],
			max_attempts=2,
			label="query_build_generate",
		)
		raw = _extract_message_content(response)
		parsed = _parse_json_response(raw)

		sql = (parsed.get("sql") or "").strip()
		if not sql:
			return {
				"error": "A LLM nao retornou SQL valido para a solicitacao.",
				"error_category": "llm_api",
				"warnings": ["Tente detalhar tabelas, campos e periodo esperado."],
				"repairable_error": False,
				"repair_attempts": repair_attempts,
				"quality_retry_count": quality_retry_count,
			}

		generated_sql = _extract_sql(sql)
		generated_sql, had_numeric_cast_fix = _fix_invalid_string_aggregates(generated_sql)
		warnings = _safe_list(parsed.get("warnings"))
		assumptions = _safe_list(parsed.get("assumptions"))

		if assert_select_only(generated_sql):
			# A LLM devolveu algo no campo "sql" (ex.: comentario explicando
			# que nao conseguiu gerar a query) sem ser uma SELECT/WITH real —
			# trata como "nao gerou SQL", nao como bloqueio de seguranca.
			return {
				"error": "A LLM nao retornou SQL valido para a solicitacao.",
				"error_category": "llm_api",
				"warnings": warnings + ["Tente detalhar tabelas, campos e periodo esperado."],
				"assumptions": assumptions,
				"dataset_tables": dataset_tables,
				"dataset_table_columns": dataset_table_columns,
				"dataset_table_meta": dataset_table_meta,
				"dataset_tables_context": dataset_context,
				"repairable_error": False,
				"repair_attempts": repair_attempts,
				"quality_retry_count": quality_retry_count,
			}

		if had_numeric_cast_fix:
			warnings.append(
				"SQL ajustada automaticamente: agregacao numerica com CAST(... AS STRING) foi convertida para SAFE_CAST numerico."
			)

		if dataset_warning:
			warnings.append(dataset_warning)

		if dataset_tables:
			invalid_refs = _find_invalid_table_references(generated_sql, dataset_tables)
			if invalid_refs:
				return {
					"error": (
						"A SQL gerada referenciou tabela(s) fora do dataset validado: "
						+ ", ".join(invalid_refs)
					),
					"error_category": "schema",
					"warnings": warnings
					+ [
						"Refine a pergunta citando os nomes das tabelas disponiveis no dataset.",
					],
					"assumptions": assumptions,
					"dataset_tables": dataset_tables,
					"dataset_table_columns": dataset_table_columns,
					"dataset_table_meta": dataset_table_meta,
					"dataset_tables_context": dataset_context,
					"repairable_error": False,
					"repair_attempts": repair_attempts,
					"quality_retry_count": quality_retry_count,
				}

		return {
			"generated_sql": generated_sql,
			"explanation": str(parsed.get("explanation") or ""),
			"assumptions": assumptions,
			"warnings": warnings,
			"dataset_tables": dataset_tables,
			"dataset_table_columns": dataset_table_columns,
			"dataset_table_meta": dataset_table_meta,
			"dataset_tables_context": dataset_context,
			"error": None,
			"error_category": "",
			"repairable_error": False,
			"repair_attempts": repair_attempts,
			"quality_retry_count": quality_retry_count,
		}
	except Exception as exc:
		return {
			"error": f"Falha ao gerar SQL: {exc}",
			"error_category": "llm_api",
			"warnings": ["Verifique se a solicitacao possui contexto suficiente."],
			"dataset_tables": dataset_tables,
			"dataset_table_columns": dataset_table_columns,
			"dataset_table_meta": dataset_table_meta,
			"dataset_tables_context": dataset_context,
			"repairable_error": False,
			"repair_attempts": repair_attempts,
			"quality_retry_count": quality_retry_count,
		}


def dry_run_generated_sql(state: QueryBuildState) -> dict[str, Any]:
	if state.error or not state.generated_sql:
		return {}

	result = dry_run_query(state.generated_sql, state.project_id)
	warnings = list(state.warnings)

	if result.error:
		warnings.append(f"Dry-run retornou erro: {result.error}")
		return {
			"dry_run_generated": result,
			"warnings": warnings,
			"error": (
				"A SQL gerada nao passou na validacao tecnica do BigQuery (dry-run). "
				"Ajuste a solicitacao para remover ambiguidades de filtros e campos."
			),
			"error_category": "bigquery_syntax",
			"repairable_error": True,
		}

	budget_bytes = int(
		get_runtime_config("QUERY_BUILD_BUDGET_BYTES", str(_DEFAULT_QUERY_BUDGET_BYTES))
	)
	if result.bytes_processed and result.bytes_processed > budget_bytes:
		gb_estimated = result.bytes_processed / (1024 ** 3)
		gb_budget = budget_bytes / (1024 ** 3)
		warnings.append(
			f"Consulta excederia o orcamento de bytes: {gb_estimated:.1f} GB estimado, limite {gb_budget:.1f} GB."
		)
		return {
			"dry_run_generated": result,
			"warnings": warnings,
			"error": (
				f"Essa consulta processaria aproximadamente {gb_estimated:.1f} GB, acima do limite de "
				f"{gb_budget:.1f} GB configurado. Tente reduzir o periodo ou adicionar filtros mais especificos."
			),
			"error_category": "budget",
			"repairable_error": False,
		}

	cost_tier = ""
	if result.bytes_processed and budget_bytes:
		pct = result.bytes_processed / budget_bytes
		cost_tier = "baixo" if pct < 0.20 else "moderado" if pct < 0.70 else "alto"

	return {
		"dry_run_generated": result,
		"warnings": warnings,
		"cost_tier": cost_tier,
	}


def _extract_where_clause(sql: str) -> str:
	match = re.search(
		r"\bWHERE\b(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|\bQUALIFY\b|$)",
		sql,
		flags=re.IGNORECASE | re.DOTALL,
	)
	return match.group(1) if match else ""


def _extract_referenced_tables(sql: str, table_meta: dict[str, dict[str, Any]]) -> list[tuple[str, str]]:
	"""Retorna [(full_name, alias_ou_vazio), ...] das tabelas referenciadas que
	têm metadados conhecidos (ignora tabelas fora do catálogo validado).
	"""
	found: list[tuple[str, str]] = []
	for table_ref, alias in re.findall(TABLE_ALIAS_PATTERN, sql, flags=re.IGNORECASE):
		full_name = table_ref.lower()
		if full_name in table_meta:
			found.append((full_name, alias or ""))
	return found


def _deterministic_quality_checks(
	sql: str,
	table_meta: dict[str, dict[str, Any]] | None = None,
) -> tuple[int, list[str]]:
	"""Checagens objetivas (regex) contra os 5 PILARES OBRIGATÓRIOS — a parte
	que não exige julgamento (isso fica para a chamada de LLM em score_query).
	"""
	score = 100
	issues: list[str] = []

	stripped = re.sub(r"'[^']*'", "''", sql)
	stripped = re.sub(r'"[^"]*"', '""', stripped)

	if re.search(INLINE_COMMENT_PATTERN, stripped, flags=re.DOTALL):
		score -= 5
		issues.append("Contem comentarios inline na SQL final (Pilar 5 - Interface).")

	no_comments = re.sub(INLINE_COMMENT_PATTERN, "", stripped, flags=re.DOTALL)

	if re.search(SELECT_STAR_PATTERN, no_comments, flags=re.IGNORECASE):
		score -= 15
		issues.append("Usa SELECT * em vez de listar colunas explicitamente (Pilar 5 - Interface).")

	if re.search(ORDER_BY_ORDINAL_PATTERN, no_comments, flags=re.IGNORECASE):
		score -= 10
		issues.append("ORDER BY por posicao ordinal em vez de alias explicito (Pilar 5 - Interface).")

	division_count = no_comments.count("/")
	nullif_count = len(re.findall(r"nullif\s*\(", no_comments, flags=re.IGNORECASE))
	if division_count > nullif_count:
		score -= min(30, 10 * (division_count - nullif_count))
		issues.append("Divisao sem NULLIF para evitar erro de divisao por zero (Pilar 3 - Estabilidade).")

	if table_meta:
		where_clause = _extract_where_clause(no_comments)
		for full_name, alias in _extract_referenced_tables(no_comments, table_meta):
			meta = table_meta.get(full_name, {})
			partition_field = str(meta.get("partition_field") or "")
			clustering_fields = list(meta.get("clustering_fields") or [])

			if partition_field:
				qualified = rf"\b{re.escape(alias)}\.{re.escape(partition_field)}\b" if alias else ""
				bare = rf"\b{re.escape(partition_field)}\b"
				pattern = rf"{qualified}|{bare}" if qualified else bare
				if not re.search(pattern, where_clause, flags=re.IGNORECASE):
					score -= 20
					issues.append(
						f"Tabela {full_name} tem coluna de particao '{partition_field}' mas a SQL nao "
						"filtra por ela no WHERE - risco de varredura completa (Pilar 1 - Performance)."
					)

			if clustering_fields:
				rest_of_query = no_comments[no_comments.upper().find("WHERE"):] if "WHERE" in no_comments.upper() else no_comments
				hits_clustering = any(
					re.search(rf"\b{re.escape(cf)}\b", rest_of_query, flags=re.IGNORECASE)
					for cf in clustering_fields
				)
				if not hits_clustering:
					score -= 5
					issues.append(
						f"Tabela {full_name} tem colunas de clustering ({', '.join(clustering_fields)}) "
						"nao utilizadas em WHERE/ORDER BY - oportunidade de eficiencia (Pilar 1 - Performance)."
					)

	return max(0, score), issues


def score_query(state: QueryBuildState, llm: BaseChatModel) -> dict[str, Any]:
	"""Nota 0-100 de boas praticas contra os 5 PILARES OBRIGATORIOS do
	QUERY_BUILD_SYSTEM_PROMPT — hibrido regra (objetiva) + LLM (julgamento).
	"""
	if state.error or not state.generated_sql:
		return {}

	score, issues = _deterministic_quality_checks(state.generated_sql, state.dataset_table_meta)

	judge_prompt = f"""
SQL gerada:
{state.generated_sql}

Pergunta original do usuario:
{state.request_text}
"""

	try:
		response = invoke_with_retry(
			llm,
			[
				SystemMessage(content=_QUALITY_JUDGE_SYSTEM_PROMPT),
				HumanMessage(content=judge_prompt),
			],
			max_attempts=2,
			label="query_build_score",
		)
		raw = _extract_message_content(response)
		judged = _parse_json_response(raw)

		if judged.get("single_scan_ok") is False:
			score -= 20
			reason = str(judged.get("single_scan_reason") or "Estrutura nao parece single-scan.")
			issues.append(f"Possivelmente nao single-scan: {reason} (Pilar 1 - Performance).")

		if judged.get("fields_coherent_ok") is False:
			score -= 20
			reason = str(judged.get("fields_coherent_reason") or "Campos podem nao corresponder a pergunta.")
			issues.append(f"Campos podem nao corresponder a pergunta: {reason} (Pilar 2 - Semantica).")
	except Exception as exc:
		issues.append(f"Avaliacao por LLM indisponivel; nota baseada apenas em checagens automaticas ({exc}).")

	return {
		"quality_score": max(0, min(100, score)),
		"quality_issues": issues,
	}


def await_quality_approval(state: QueryBuildState) -> dict[str, Any]:
	"""HITL nativo do LangGraph — interrupt() pausa de verdade o grafo até
	`QueryBuildAgent.resume()` ser chamado com a decisao humana.
	"""
	min_score = int(get_runtime_config("QUERY_BUILD_MIN_QUALITY_SCORE", "80"))
	if state.quality_score >= min_score:
		return {"human_decision": "skip"}

	if state.quality_retry_count >= 2:
		warnings = list(state.warnings)
		warnings.append(
			f"Nao foi possivel elevar a nota de qualidade acima de {min_score} apos 2 tentativas de melhoria; "
			f"seguindo com a melhor versao obtida (nota atual: {state.quality_score})."
		)
		return {"human_decision": "skip", "warnings": warnings}

	decision = interrupt({
		"message": f"A consulta gerada tem nota {state.quality_score}/100. Deseja seguir assim ou melhorar?",
		"score": state.quality_score,
		"issues": state.quality_issues,
	})
	return {"human_decision": decision}


def validate_generated_sql_consistency(state: QueryBuildState) -> dict[str, Any]:
	if state.error or not state.generated_sql:
		return {}

	safety_error = assert_select_only(state.generated_sql)
	if safety_error:
		warnings = list(state.warnings)
		warnings.append(f"SQL bloqueada por seguranca: {safety_error}")
		return {
			"warnings": warnings,
			"error": safety_error,
			"error_category": "sql_safety",
			"sample_error": "Amostra bloqueada: SQL reprovada na checagem de seguranca.",
			"repairable_error": False,
		}

	template_params = _find_template_placeholders(state.generated_sql)
	if template_params:
		param_list = ", ".join(template_params)
		warnings = list(state.warnings)
		warnings.append(
			"SQL bloqueada por validacao: placeholder(s) de template nao resolvido(s): "
			+ param_list
		)

		assumptions = list(state.assumptions)
		assumptions.append(
			"A pergunta exige periodo/valor dinamico sem literal definido; informe as datas/limites explicitamente na solicitacao."
		)

		return {
			"warnings": warnings,
			"assumptions": assumptions,
			"error": (
				"A SQL contem placeholder(s) de template sem resolucao ("
				+ param_list
				+ "). "
				"Para garantir execucao consistente, informe valores literais (ex.: datas e limites) na solicitacao."
			),
			"error_category": "schema",
			"sample_error": "Amostra bloqueada: SQL com placeholder de template sem valor.",
			"repairable_error": True,
		}

	named_params = _find_named_parameters(state.generated_sql)
	if named_params:
		param_list = ", ".join(sorted(named_params))
		warnings = list(state.warnings)
		warnings.append(
			"SQL bloqueada por validacao: placeholder(s) de parametro nao resolvido(s): "
			+ param_list
		)

		assumptions = list(state.assumptions)
		assumptions.append(
			"A pergunta nao trouxe valor literal para filtro parametrico; informe explicitamente o limite numerico desejado."
		)

		return {
			"warnings": warnings,
			"assumptions": assumptions,
			"error": (
				"A SQL contem parametro(s) nomeado(s) sem valor ("
				+ param_list
				+ "). "
				"Para garantir consistencia de execucao, informe o valor literal do filtro na solicitacao."
			),
			"error_category": "schema",
			"sample_error": "Amostra bloqueada: SQL com parametro sem valor.",
			"repairable_error": True,
		}

	invalid_columns = _find_invalid_column_references(
		state.generated_sql,
		state.dataset_table_columns,
	)
	if not invalid_columns:
		return {}

	warnings = list(state.warnings)
	warnings.append(
		"SQL bloqueada por validacao semantica: coluna(s) nao encontradas no schema: "
		+ ", ".join(invalid_columns)
	)

	return {
		"warnings": warnings,
		"error": (
			"A SQL referencia coluna(s) inexistente(s) no schema validado: "
			+ ", ".join(invalid_columns)
			+ ". Revise a solicitacao com nomes de campos reais do dataset."
		),
		"error_category": "schema",
		"sample_error": "Amostra bloqueada: SQL com coluna inexistente no schema.",
		"repairable_error": True,
	}


def review_and_optimize_sql(state: QueryBuildState, llm: BaseChatModel) -> dict[str, Any]:
	if state.error or not state.generated_sql:
		return {}

	context = state.dataset_tables_context or ""
	review_prompt = f"""
SQL atual:
{state.generated_sql}

Contexto adicional:
{context or '(sem contexto adicional)'}

Regras obrigatorias:
- Nao altere o significado de negocio da query.
- Preserve as tabelas reais do dataset validado.
- Remova redundancias, simplifique blocos e mantenha single scan.
- Use NULLIF em divisoes.
"""

	try:
		response = invoke_with_retry(
			llm,
			[
				SystemMessage(content=QUERY_BUILD_REVIEWER_PROMPT),
				HumanMessage(content=review_prompt),
			],
			max_attempts=2,
			label="query_build_review",
		)
		reviewed_raw = _extract_message_content(response)
		reviewed_sql = _extract_sql(reviewed_raw)
		reviewed_sql, had_numeric_cast_fix = _fix_invalid_string_aggregates(reviewed_sql)

		if not reviewed_sql:
			return {
				"warnings": list(state.warnings)
				+ ["Reviewer nao retornou SQL valida; mantendo versao original."],
			}

		if state.dataset_tables:
			invalid_refs = _find_invalid_table_references(reviewed_sql, state.dataset_tables)
			if invalid_refs:
				return {
					"warnings": list(state.warnings)
					+ [
						"Reviewer propôs tabela fora do dataset; mantendo SQL original: "
						+ ", ".join(invalid_refs),
					],
				}

		fix_warning = []
		if had_numeric_cast_fix:
			fix_warning.append(
				"Reviewer retornou agregacao numerica com CAST(... AS STRING); ajuste automatico aplicado com SAFE_CAST numerico."
			)

		return {
			"generated_sql": reviewed_sql,
			"warnings": list(state.warnings)
			+ ["SQL revisada por no de otimizacao antes do dry-run."]
			+ fix_warning,
		}
	except Exception as exc:
		return {
			"warnings": list(state.warnings)
			+ [f"Falha no reviewer SQL; mantendo versao original: {exc}"],
		}


def fetch_generated_sample(state: QueryBuildState) -> dict[str, Any]:
	if state.error or not state.generated_sql:
		return {
			"sample_columns": [],
			"sample_rows": [],
			"sample_error": "Nao foi possivel executar amostra de dados sem SQL valida.",
		}

	if state.dry_run_generated and state.dry_run_generated.error:
		return {
			"sample_columns": [],
			"sample_rows": [],
			"sample_error": "Dry-run falhou; amostra de dados nao foi executada.",
		}

	sample = fetch_query_sample(state.generated_sql, state.project_id, limit=10)
	return {
		"sample_columns": sample.get("columns") or [],
		"sample_rows": sample.get("rows") or [],
		"sample_error": sample.get("error"),
	}


def record_audit(state: QueryBuildState) -> dict[str, Any]:
	"""Fan-in final — roda sempre, sucesso ou erro, pra auditoria nunca
	depender de o pipeline ter chegado inteiro até o fim.

	Reaproveita a mesma `finance_audit_log` do Finance Auditor: o Query
	Builder não tem o conceito de "plano com vários steps" daquele agente,
	então representa a si mesmo como um plano de 1 step só.
	"""
	dry = state.dry_run_generated
	tool_results = [{
		"ok": bool(state.generated_sql) and not state.error,
		"payload": {
			"bytes_processed": dry.bytes_processed if dry and not dry.error else 0,
			"estimated_cost_usd": dry.estimated_cost_usd if dry and not dry.error else 0,
		},
	}]
	plan = [{
		"capability": "query_build_generate_sql",
		"sql": state.generated_sql or "",
		"quality_score": state.quality_score,
		"quality_issues": state.quality_issues,
		"error_category": state.error_category,
	}]
	record_audit_entry({
		"user_id": str((state.user or {}).get("username") or (state.user or {}).get("user_id") or ""),
		"request_text": state.request_text,
		"plan": plan,
		"tool_results": tool_results,
		"error": state.error or "",
	})
	return {}


def _extract_message_content(response: Any) -> str:
	if hasattr(response, "content"):
		return str(response.content).strip()
	return str(response).strip()


def _parse_json_response(raw: str) -> dict[str, Any]:
	cleaned = raw.strip()
	cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
	cleaned = re.sub(r"\s*```$", "", cleaned)

	try:
		return json.loads(cleaned)
	except Exception:
		return {
			"sql": _extract_sql(cleaned),
			"explanation": "SQL retornado sem JSON estruturado.",
			"assumptions": [],
			"warnings": ["Resposta da LLM fora do formato esperado."],
		}


def _safe_list(value: Any) -> list[str]:
	if isinstance(value, list):
		return [str(item) for item in value if str(item).strip()]
	if isinstance(value, str) and value.strip():
		return [value.strip()]
	return []


def _extract_sql(raw: str) -> str:
	sql_match = re.search(SQL_FENCE_PATTERN, raw, re.IGNORECASE)
	if sql_match:
		return sql_match.group(1).strip()
	return raw.strip()


def _fix_invalid_string_aggregates(sql: str) -> tuple[str, bool]:
	changed = False

	def _replacer(match: re.Match[str]) -> str:
		nonlocal changed
		agg = match.group(1).upper()
		expr = match.group(2).strip()
		changed = True

		if agg == "AVG":
			return f"AVG(SAFE_CAST({expr} AS FLOAT64))"
		if agg == "SUM":
			return f"SUM(SAFE_CAST({expr} AS NUMERIC))"
		return f"{agg}(SAFE_CAST({expr} AS FLOAT64))"

	fixed = re.sub(INVALID_STRING_AGG_PATTERN, _replacer, sql, flags=re.IGNORECASE)
	return fixed, changed


def _find_invalid_table_references(sql: str, allowed_tables: list[str]) -> list[str]:
	allowed = {table.lower() for table in allowed_tables}
	found = {ref.lower() for ref in re.findall(TABLE_REF_PATTERN, sql)}

	invalid = sorted(found - allowed)
	return invalid


def _find_named_parameters(sql: str) -> list[str]:
	stripped_sql = re.sub(r"'[^']*'", "''", sql)
	stripped_sql = re.sub(r'"[^"]*"', '""', stripped_sql)
	params = {match.group(1) for match in re.finditer(NAMED_PARAM_PATTERN, stripped_sql)}
	return sorted(params)


def _find_template_placeholders(sql: str) -> list[str]:
	mustache = {match.group(1).strip() for match in re.finditer(MUSTACHE_PARAM_PATTERN, sql)}
	dollar = {match.group(1).strip() for match in re.finditer(DOLLAR_PARAM_PATTERN, sql)}
	params = {param for param in mustache.union(dollar) if param}
	return sorted(params)


def _build_table_columns_map(tables: list[dict[str, Any]]) -> dict[str, list[str]]:
	columns_map: dict[str, list[str]] = {}
	for item in tables:
		full_name = str(item.get("full_name") or "").strip()
		if not full_name:
			continue
		cols: list[str] = []
		for col in (item.get("columns") or []):
			# Schema returns list[dict] with {name, type, mode, description}
			# Metadata returns list[str]
			if isinstance(col, dict):
				name = str(col.get("name") or "").strip()
			else:
				name = str(col).strip()
			if name:
				cols.append(name)
		columns_map[full_name.lower()] = cols
	return columns_map


def _build_table_meta_map(tables: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
	meta_map: dict[str, dict[str, Any]] = {}
	for item in tables:
		full_name = str(item.get("full_name") or "").strip()
		if not full_name:
			continue
		meta_map[full_name.lower()] = {
			"partition_field": str(item.get("partition_field") or "").strip(),
			"clustering_fields": list(item.get("clustering_fields") or []),
		}
	return meta_map


def _find_invalid_column_references(
	sql: str,
	table_columns: dict[str, list[str]],
) -> list[str]:
	if not table_columns:
		return []

	normalized: dict[str, set[str]] = {
		table.lower(): {col.lower() for col in cols}
		for table, cols in table_columns.items()
	}

	table_tail_to_full: dict[str, str] = {}
	ambiguous_tails: set[str] = set()
	for full_name in normalized:
		tail = full_name.split(".")[-1]
		if tail in table_tail_to_full and table_tail_to_full[tail] != full_name:
			ambiguous_tails.add(tail)
		else:
			table_tail_to_full[tail] = full_name
	for tail in ambiguous_tails:
		table_tail_to_full.pop(tail, None)

	alias_to_table: dict[str, str] = {}
	for table_ref, alias in re.findall(TABLE_ALIAS_PATTERN, sql, flags=re.IGNORECASE):
		full_ref = table_ref.lower()
		if full_ref not in normalized:
			continue

		tail = full_ref.split(".")[-1]
		alias_to_table[tail] = full_ref
		if alias:
			alias_to_table[alias.lower()] = full_ref

	stripped_sql = re.sub(r"'[^']*'", "''", sql)
	stripped_sql = re.sub(r'"[^"]*"', '""', stripped_sql)
	stripped_sql = re.sub(TABLE_REF_PATTERN, "", stripped_sql)

	invalid: set[str] = set()
	for lhs, column in re.findall(QUALIFIED_COLUMN_PATTERN, stripped_sql):
		lhs_l = lhs.lower()
		column_l = column.lower()

		full_table = alias_to_table.get(lhs_l) or table_tail_to_full.get(lhs_l)
		if not full_table:
			continue

		if column_l not in normalized.get(full_table, set()):
			invalid.add(f"{lhs}.{column}")

	return sorted(invalid)
