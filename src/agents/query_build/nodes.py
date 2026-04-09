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


def generate_sql_from_request(state: QueryBuildState, llm: BaseChatModel) -> dict[str, Any]:
	dataset_tables: list[str] = []
	dataset_context = ""
	dataset_warning: str | None = None

	if state.dataset_hint:
		try:
			metadata = get_dataset_tables_metadata(state.project_id, state.dataset_hint)
			dataset_tables = [item["full_name"] for item in metadata.get("tables", [])]

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
		warnings = _safe_list(parsed.get("warnings"))
		assumptions = _safe_list(parsed.get("assumptions"))

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
					"dataset_tables_context": dataset_context,
				}

		return {
			"generated_sql": generated_sql,
			"explanation": str(parsed.get("explanation") or ""),
			"assumptions": assumptions,
			"warnings": warnings,
			"dataset_tables": dataset_tables,
			"dataset_tables_context": dataset_context,
		}
	except Exception as exc:
		return {
			"error": f"Falha ao gerar SQL: {exc}",
			"warnings": ["Verifique se a solicitacao possui contexto suficiente."],
			"dataset_tables": dataset_tables,
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

		return {
			"generated_sql": reviewed_sql,
			"warnings": list(state.warnings)
			+ ["SQL revisada por no de otimizacao antes do dry-run."],
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


def _find_invalid_table_references(sql: str, allowed_tables: list[str]) -> list[str]:
	allowed = {table.lower() for table in allowed_tables}
	found = {ref.lower() for ref in re.findall(TABLE_REF_PATTERN, sql)}

	invalid = sorted(found - allowed)
	return invalid
