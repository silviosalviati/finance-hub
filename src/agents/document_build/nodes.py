from __future__ import annotations

import json
import re
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.document_build.prompts import DOCUMENT_BUILD_SYSTEM_PROMPT
from src.agents.document_build.state import DocumentBuildState


def parse_document_request(state: DocumentBuildState) -> dict[str, Any]:
	text = (state.request_text or "").strip()
	if not text:
		return {"error": "Descreva o contexto para gerar a documentacao."}

	normalized = text.lower()
	doc_type = "documentacao_funcional"
	if any(word in normalized for word in ["runbook", "operacao", "incidente", "suporte"]):
		doc_type = "runbook_operacional"
	elif any(word in normalized for word in ["especificacao", "arquitetura", "tecnica"]):
		doc_type = "especificacao_tecnica"
	elif any(word in normalized for word in ["guia", "implementacao", "passo a passo"]):
		doc_type = "guia_implementacao"

	title = f"Document Build - {doc_type.replace('_', ' ').title()}"
	metadata = {
		"project_id": state.project_id,
		"dataset_hint": state.dataset_hint or "nao informado",
	}

	return {
		"doc_type": doc_type,
		"title": title,
		"metadata": metadata,
	}


def generate_document_structure(state: DocumentBuildState, llm: BaseChatModel) -> dict[str, Any]:
	if state.error:
		return {}

	prompt = f"""
Contexto informado pelo usuario:
{state.request_text}

Parametros:
- Tipo de documento: {state.doc_type}
- Project ID: {state.project_id}
- Dataset hint: {state.dataset_hint or 'nao informado'}

Gere a documentacao completa no formato solicitado.
"""

	try:
		response = llm.invoke(
			[
				SystemMessage(content=DOCUMENT_BUILD_SYSTEM_PROMPT),
				HumanMessage(content=prompt),
			]
		)

		raw = _extract_message_content(response)
		payload = _parse_json_response(raw)

		sections = _normalize_sections(payload.get("sections"))
		if not sections:
			sections = [
				{
					"title": "Contexto",
					"content": state.request_text,
				}
			]

		return {
			"title": str(payload.get("title") or state.title),
			"doc_type": str(payload.get("doc_type") or state.doc_type),
			"summary": str(payload.get("summary") or "Documentacao gerada a partir do contexto informado."),
			"audience": str(payload.get("audience") or "Times de Dados, BI e Engenharia Analytics"),
			"sections": sections,
			"assumptions": _safe_list(payload.get("assumptions")),
			"risks": _safe_list(payload.get("risks")),
			"acceptance_checklist": _safe_list(payload.get("acceptance_checklist")),
			"next_steps": _safe_list(payload.get("next_steps")),
			"warnings": _safe_list(payload.get("warnings")),
		}
	except Exception as exc:
		return {
			"error": f"Falha ao gerar documentacao: {exc}",
			"warnings": ["Revise o contexto e tente novamente com mais detalhes."],
		}


def finalize_document_markdown(state: DocumentBuildState) -> dict[str, Any]:
	if state.error:
		return {}

	lines: list[str] = [
		f"# {state.title or 'Documentacao Tecnica'}",
		"",
		f"**Tipo:** {state.doc_type}",
		f"**Publico-alvo:** {state.audience or 'Times tecnicos'}",
		f"**Project ID:** {state.metadata.get('project_id', state.project_id)}",
		f"**Dataset hint:** {state.metadata.get('dataset_hint', state.dataset_hint or 'nao informado')}",
		"",
		"## Resumo executivo",
		state.summary or "Sem resumo informado.",
		"",
	]

	for section in state.sections:
		title = section.get("title", "Secao")
		content = section.get("content", "")
		lines.append(f"## {title}")
		lines.append(content or "Sem conteudo informado.")
		lines.append("")

	_append_list_section(lines, "Premissas", state.assumptions)
	_append_list_section(lines, "Riscos", state.risks)
	_append_list_section(lines, "Checklist de aceitacao", state.acceptance_checklist)
	_append_list_section(lines, "Proximos passos", state.next_steps)

	if state.warnings:
		_append_list_section(lines, "Observacoes", state.warnings)

	markdown = "\n".join(lines).strip() + "\n"
	quality_score = _estimate_quality_score(state)

	return {
		"markdown_document": markdown,
		"quality_score": quality_score,
	}


def _append_list_section(lines: list[str], title: str, values: list[str]) -> None:
	lines.append(f"## {title}")
	if not values:
		lines.append("- Nao informado")
	else:
		for item in values:
			lines.append(f"- {item}")
	lines.append("")


def _estimate_quality_score(state: DocumentBuildState) -> int:
	score = 40
	score += min(len(state.sections) * 8, 24)
	score += min(len(state.acceptance_checklist) * 3, 12)
	score += min(len(state.next_steps) * 3, 12)
	score += min(len(state.risks) * 2, 8)
	score += 4 if state.summary else 0
	score -= min(len(state.warnings) * 3, 12)
	return max(0, min(100, score))


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
			"title": "Documentacao Tecnica",
			"summary": "Resposta retornada fora do formato JSON. Conteudo aproveitado de forma parcial.",
			"sections": [
				{
					"title": "Conteudo bruto",
					"content": cleaned,
				}
			],
			"warnings": ["A resposta da LLM nao seguiu o contrato JSON esperado."],
		}


def _normalize_sections(value: Any) -> list[dict[str, str]]:
	if not isinstance(value, list):
		return []

	sections: list[dict[str, str]] = []
	for section in value:
		if not isinstance(section, dict):
			continue
		title = str(section.get("title") or "").strip()
		content = str(section.get("content") or "").strip()
		if not title and not content:
			continue
		sections.append(
			{
				"title": title or "Secao",
				"content": content or "Sem conteudo informado.",
			}
		)
	return sections


def _safe_list(value: Any) -> list[str]:
	if isinstance(value, list):
		return [str(item).strip() for item in value if str(item).strip()]
	if isinstance(value, str) and value.strip():
		return [value.strip()]
	return []
