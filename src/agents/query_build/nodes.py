from __future__ import annotations

import json
import re
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.query_build.prompts import (
	QUERY_BUILD_REVIEWER_PROMPT,
	QUERY_BUILD_SYSTEM_PROMPT,
)
from src.agents.query_build.state import QueryBuildState
from src.shared.tools.bigquery import (
	fetch_query_sample,
	dry_run_query,
	get_dataset_tables_metadata,
)

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


def generate_sql_from_request(state: QueryBuildState, llm: BaseChatModel) -> dict[str, Any]:
	dataset_tables: list[str] = []
	dataset_table_columns: dict[str, list[str]] = {}
	dataset_context = ""
	dataset_warning: str | None = None

	if state.dataset_hint:
		try:
			metadata = get_dataset_tables_metadata(state.project_id, state.dataset_hint)
			dataset_tables = [item["full_name"] for item in metadata.get("tables", [])]
			dataset_table_columns = _build_table_columns_map(metadata.get("tables", []))

			if dataset_tables:
				lines = [
					"Catalogo real de tabelas disponiveis no dataset (use somente estas tabelas):"
				]
				for item in metadata.get("tables", []):
					columns = item.get("columns") or []
					cols = ", ".join(columns) if columns else "(colunas nao carregadas)"
					lines.append(f"- {item['full_name']} | colunas: {cols}")
				dataset_context = "\n".join(lines)
			else:
				dataset_warning = (
					"Dataset informado nao retornou tabelas visiveis para o service account."
				)
		except Exception as exc:
			dataset_warning = f"Nao foi possivel carregar tabelas reais do dataset: {exc}"

	user_prompt = f"""
Solicitacao do usuario:
{state.request_text}

Project ID:
{state.project_id}

Dataset hint (opcional):
{state.dataset_hint or '(nao informado)'}

{dataset_context or 'Catalogo de tabelas indisponivel para esta solicitacao.'}

Regra obrigatoria:
- Use apenas tabelas reais listadas no catalogo acima.
- Nao invente nome de tabela.
- Valide tipo de colunas no schema fornecido antes de montar JOINs e filtros.
- Em colunas equivalentes com tipos diferentes (ex.: STRING vs INT64), use CAST explicito para compatibilizar.
"""

	try:
		response = llm.invoke(
			[
				SystemMessage(content=QUERY_BUILD_SYSTEM_PROMPT),
				HumanMessage(content=user_prompt),
			]
		)
		raw = _extract_message_content(response)
		parsed = _parse_json_response(raw)

		sql = (parsed.get("sql") or "").strip()
		if not sql:
			return {
				"error": "A LLM nao retornou SQL valido para a solicitacao.",
				"warnings": ["Tente detalhar tabelas, campos e periodo esperado."],
			}

		generated_sql = _extract_sql(sql)
		generated_sql, had_numeric_cast_fix = _fix_invalid_string_aggregates(generated_sql)
		warnings = _safe_list(parsed.get("warnings"))
		assumptions = _safe_list(parsed.get("assumptions"))

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
					"warnings": warnings
					+ [
						"Refine a pergunta citando os nomes das tabelas disponiveis no dataset.",
					],
					"assumptions": assumptions,
					"dataset_tables": dataset_tables,
					"dataset_table_columns": dataset_table_columns,
					"dataset_tables_context": dataset_context,
				}

		return {
			"generated_sql": generated_sql,
			"explanation": str(parsed.get("explanation") or ""),
			"assumptions": assumptions,
			"warnings": warnings,
			"dataset_tables": dataset_tables,
			"dataset_table_columns": dataset_table_columns,
			"dataset_tables_context": dataset_context,
		}
	except Exception as exc:
		return {
			"error": f"Falha ao gerar SQL: {exc}",
			"warnings": ["Verifique se a solicitacao possui contexto suficiente."],
			"dataset_tables": dataset_tables,
			"dataset_table_columns": dataset_table_columns,
			"dataset_tables_context": dataset_context,
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
		}

	return {
		"dry_run_generated": result,
		"warnings": warnings,
	}


def validate_generated_sql_consistency(state: QueryBuildState) -> dict[str, Any]:
	if state.error or not state.generated_sql:
		return {}

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
			"sample_error": "Amostra bloqueada: SQL com placeholder de template sem valor.",
		}

	named_params = _find_named_parameters(state.generated_sql)
	if not named_params:
		pass
	else:
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
			"sample_error": "Amostra bloqueada: SQL com parametro sem valor.",
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
		"sample_error": "Amostra bloqueada: SQL com coluna inexistente no schema.",
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
		response = llm.invoke(
			[
				SystemMessage(content=QUERY_BUILD_REVIEWER_PROMPT),
				HumanMessage(content=review_prompt),
			]
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
		cols = [str(col).strip() for col in (item.get("columns") or []) if str(col).strip()]
		columns_map[full_name.lower()] = cols
	return columns_map


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
