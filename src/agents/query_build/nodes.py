from __future__ import annotations

import json
import re
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.query_build.prompts import QUERY_BUILD_SYSTEM_PROMPT
from src.agents.query_build.state import QueryBuildState
from src.shared.tools.bigquery import dry_run_query

SQL_FENCE_PATTERN = r"```sql\s*([\s\S]+?)\s*```"


def generate_sql_from_request(state: QueryBuildState, llm: BaseChatModel) -> dict[str, Any]:
	user_prompt = f"""
Solicitacao do usuario:
{state.request_text}

Project ID:
{state.project_id}

Dataset hint (opcional):
{state.dataset_hint or '(nao informado)'}
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

		return {
			"generated_sql": _extract_sql(sql),
			"explanation": str(parsed.get("explanation") or ""),
			"assumptions": _safe_list(parsed.get("assumptions")),
			"warnings": _safe_list(parsed.get("warnings")),
		}
	except Exception as exc:
		return {
			"error": f"Falha ao gerar SQL: {exc}",
			"warnings": ["Verifique se a solicitacao possui contexto suficiente."],
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
