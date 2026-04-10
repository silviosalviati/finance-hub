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
	if "perfil comportamental" in normalized:
		title = "Documentacao Tecnica - Pipeline Perfil Comportamental de Clientes"

	frequency = "Batch diario"
	if any(word in normalized for word in ["stream", "streaming", "tempo real", "real-time"]):
		frequency = "Streaming"
	elif any(word in normalized for word in ["horario", "hourly"]):
		frequency = "Batch horario"

	objective = (
		"Centralizar indicadores de risco e comportamento para suportar credito, marketing e monitoramento operacional."
	)
	metadata = {
		"project_id": state.project_id,
		"dataset_hint": state.dataset_hint or "nao informado",
	}

	return {
		"doc_type": doc_type,
		"title": title,
		"objective": objective,
		"frequency": frequency,
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
		data_dictionary = _normalize_data_dictionary(payload.get("data_dictionary"))
		acceptance_checklist = _safe_list(payload.get("acceptance_checklist"))
		governance = _normalize_governance(payload.get("governance"))

		enriched = _enrich_required_blocks(
			request_text=state.request_text,
			sections=sections,
			data_dictionary=data_dictionary,
			acceptance_checklist=acceptance_checklist,
			next_steps=_safe_list(payload.get("next_steps")),
			governance=governance,
		)

		return {
			"title": str(payload.get("title") or state.title),
			"doc_type": str(payload.get("doc_type") or state.doc_type),
			"summary": str(payload.get("summary") or "Documentacao gerada a partir do contexto informado."),
			"audience": str(payload.get("audience") or "Times de Dados, BI e Engenharia Analytics"),
			"objective": str(payload.get("objective") or state.objective),
			"frequency": str(payload.get("frequency") or state.frequency or "Batch diario"),
			"sections": enriched["sections"],
			"data_dictionary": enriched["data_dictionary"],
			"assumptions": _safe_list(payload.get("assumptions")),
			"risks": _safe_list(payload.get("risks")),
			"acceptance_checklist": enriched["acceptance_checklist"],
			"next_steps": enriched["next_steps"],
			"warnings": _safe_list(payload.get("warnings")),
			"governance": enriched["governance"],
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
		"## 1. Visao geral (Overview)",
		f"**Objetivo:** {state.objective or 'Nao informado'}",
		f"**Frequencia:** {state.frequency or 'Nao informado'}",
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

	lines.append("## 2. Dicionario de dados (Data Dictionary)")
	if state.data_dictionary:
		lines.append("| Coluna | Tipo | Descricao | Regra de Negocio |")
		lines.append("| --- | --- | --- | --- |")
		for row in state.data_dictionary:
			col = row.get("column", "-")
			typ = row.get("type", "-")
			desc = row.get("description", "-")
			rule = row.get("business_rule", "-")
			lines.append(f"| {col} | {typ} | {desc} | {rule} |")
	else:
		lines.append("- Nao informado")
	lines.append("")

	lines.append("## 3. Checklist de qualidade de dados (DQ)")
	if state.acceptance_checklist:
		for item in state.acceptance_checklist:
			lines.append(f"- [ ] {item}")
	else:
		lines.append("- [ ] Nao informado")
	lines.append("")

	lines.append("## 4. Governanca (Dataplex/Catalog)")
	aspects = state.governance.get("aspect_types") if isinstance(state.governance, dict) else []
	readers = state.governance.get("readers") if isinstance(state.governance, dict) else []
	notes = state.governance.get("notes") if isinstance(state.governance, dict) else []
	lines.append("**Aspect Types**")
	if aspects:
		for item in aspects:
			lines.append(f"- {item}")
	else:
		lines.append("- Nao informado")
	lines.append("")
	lines.append("**Permissoes de leitura**")
	if readers:
		for item in readers:
			lines.append(f"- {item}")
	else:
		lines.append("- Nao informado")
	if notes:
		lines.append("")
		lines.append("**Notas de governanca**")
		for item in notes:
			lines.append(f"- {item}")
	lines.append("")

	_append_list_section(lines, "Premissas", state.assumptions)
	_append_list_section(lines, "Riscos", state.risks)
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
	score += min(len(state.data_dictionary) * 3, 12)
	score += min(len(state.acceptance_checklist) * 3, 12)
	score += min(len(state.next_steps) * 3, 12)
	score += min(len(state.risks) * 2, 8)
	if isinstance(state.governance, dict) and state.governance.get("aspect_types"):
		score += 6
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
			"objective": "Consolidar documentacao tecnica operacional e de negocio.",
			"frequency": "Batch diario",
			"sections": [
				{
					"title": "Conteudo bruto",
					"content": cleaned,
				}
			],
			"data_dictionary": [],
			"governance": {
				"aspect_types": [],
				"readers": [],
				"notes": [],
			},
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


def _normalize_data_dictionary(value: Any) -> list[dict[str, str]]:
	if not isinstance(value, list):
		return []

	rows: list[dict[str, str]] = []
	for item in value:
		if not isinstance(item, dict):
			continue
		column = str(item.get("column") or "").strip()
		type_name = str(item.get("type") or "").strip()
		description = str(item.get("description") or "").strip()
		business_rule = str(item.get("business_rule") or "").strip()
		if not any([column, type_name, description, business_rule]):
			continue
		rows.append(
			{
				"column": column or "campo",
				"type": type_name or "STRING",
				"description": description or "Sem descricao.",
				"business_rule": business_rule or "Sem regra de negocio.",
			}
		)
	return rows


def _normalize_governance(value: Any) -> dict[str, list[str]]:
	if not isinstance(value, dict):
		return {"aspect_types": [], "readers": [], "notes": []}

	return {
		"aspect_types": _safe_list(value.get("aspect_types")),
		"readers": _safe_list(value.get("readers")),
		"notes": _safe_list(value.get("notes")),
	}


def _enrich_required_blocks(
	request_text: str,
	sections: list[dict[str, str]],
	data_dictionary: list[dict[str, str]],
	acceptance_checklist: list[str],
	next_steps: list[str],
	governance: dict[str, list[str]],
) -> dict[str, Any]:
	normalized = request_text.lower()

	if not sections:
		sections = [
			{
				"title": "Arquitetura da pipeline",
				"content": (
					"Fluxo de ingestao e enriquecimento com camadas de validacao, regras de negocio e publicacao para consumo analitico."
				),
			}
		]

	if "perfil_comportamental_clientes" in normalized and not data_dictionary:
		data_dictionary = [
			{
				"column": "cliente_id",
				"type": "INTEGER",
				"description": "Identificador unico do cliente.",
				"business_rule": "Deve ser cast para INTEGER para evitar falhas de JOIN por tipo divergente.",
			},
			{
				"column": "score_credito",
				"type": "INTEGER",
				"description": "Score consolidado de risco de credito.",
				"business_rule": "Valor deve ficar no intervalo entre 0 e 1000.",
			},
			{
				"column": "segmento_cliente",
				"type": "STRING",
				"description": "Classificacao de perfil para estrategia comercial.",
				"business_rule": "Nao pode ser nulo para registros ativos.",
			},
		]

	mandatory_checks = [
		"O cliente_id e unico (Primary Key)?",
		"O score_credito esta entre 0 e 1000?",
		"Existem valores nulos em campos criticos de segmentacao?",
	]
	for check in mandatory_checks:
		if check not in acceptance_checklist:
			acceptance_checklist.append(check)

	if not governance.get("aspect_types"):
		governance["aspect_types"] = [
			"schema_contract",
			"data_quality_profile",
			"data_owner",
		]
	if not governance.get("readers"):
		governance["readers"] = ["Service Account bot-query"]
	if not governance.get("notes"):
		governance["notes"] = [
			"Habilitar monitoramento de desvio de esquema no Dataplex para alertas proativos.",
		]

	if not next_steps:
		next_steps = [
			"Configurar alerta de schema drift no Dataplex para a tabela principal.",
			"Publicar runbook de tratamento para eventos de quebra de contrato.",
			"Definir dono de dados e SLA de correcao para incidentes de DQ.",
		]

	return {
		"sections": sections,
		"data_dictionary": data_dictionary,
		"acceptance_checklist": acceptance_checklist,
		"next_steps": next_steps,
		"governance": governance,
	}
