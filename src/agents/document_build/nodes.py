from __future__ import annotations

import json
import re
from typing import Any

from google.api_core.exceptions import NotFound
from google.cloud import dataplex_v1
from google.oauth2 import service_account
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.document_build.prompts import DOCUMENT_BUILD_SYSTEM_PROMPT
from src.agents.document_build.state import DocumentBuildState
from src.shared.config import GCP_CREDENTIALS_PATH
from src.shared.tools.bigquery import get_dataset_tables_schema


_EMPTY_MARKERS = {"nenhum", "none", "n/a", "não informado", "nao informado", "-"}
_SUMMARY_SECTION_TITLES: set[str] = {
	"passos operacionais",
	"sumario",
	"sumário",
	"indice",
	"índice",
	"resumo",
	"resumo dos passos",
	"lista de passos",
	"overview dos passos",
	"visao geral dos passos",
	"visão geral dos passos",
	"passos",
	"etapas",
}


def parse_document_request(state: DocumentBuildState) -> dict[str, Any]:
	text = (state.request_text or "").strip()
	if not text:
		return {"error": "Descreva o contexto para gerar a documentacao."}

	blocks = _extract_structured_input_blocks(text)
	table_block = blocks.get("tabela") or ""
	objective_block = blocks.get("objetivo") or ""
	business_context_block = blocks.get("contexto_negocio") or ""
	doc_type_block = (blocks.get("tipo_doc") or "").lower()

	normalized = text.lower()
	doc_type = "documentacao_funcional"
	if doc_type_block in {
		"especificacao_tecnica",
		"documentacao_funcional",
		"runbook_operacional",
	}:
		doc_type = doc_type_block
	elif doc_type_block in {"especificacao tecnica", "documentacao funcional", "runbook operacional"}:
		doc_type = doc_type_block.replace(" ", "_")
	if any(word in normalized for word in ["data dictionary", "dicionario", "dicionario de dados"]):
		doc_type = "data_dictionary"
	elif any(word in normalized for word in ["contract", "data contract", "pipeline", "sla", "especificacao de pipeline"]):
		doc_type = "pipeline_data_contract"
	elif any(word in normalized for word in ["schema contract", "qualidade", "data quality", "dq", "great expectations"]):
		doc_type = "schema_contract"
	if any(word in normalized for word in ["runbook", "operacao", "incidente", "suporte"]):
		doc_type = "runbook_operacional"

	explicit_ref = _extract_explicit_table_reference(table_block or text)
	table_name = explicit_ref.get("table") or ""
	dataset_name = (state.dataset_hint or "").strip()
	if not dataset_name and explicit_ref.get("dataset"):
		dataset_name = str(explicit_ref.get("dataset") or "").strip()
	table_path = f"{state.project_id}.{dataset_name}.{table_name}" if (table_name and dataset_name) else ""

	title = f"Document Build - {doc_type.replace('_', ' ').title()}"
	if "perfil comportamental" in normalized:
		title = "Documentacao Tecnica - Pipeline Perfil Comportamental de Clientes"

	frequency = "Batch diario"
	if any(word in normalized for word in ["stream", "streaming", "tempo real", "real-time"]):
		frequency = "Streaming"
	elif any(word in normalized for word in ["horario", "hourly"]):
		frequency = "Batch horario"

	objective = objective_block or (
		"Centralizar indicadores de risco e comportamento para suportar credito, marketing e monitoramento operacional."
	)
	metadata: dict[str, Any] = {
		"project_id": state.project_id,
		"dataset_hint": state.dataset_hint or "nao informado",
		"table_name": table_name,
		"table_path": table_path,
		"explicit_table_ref": explicit_ref,
	}
	warnings: list[str] = []

	if not state.dataset_hint:
		warnings.append(
			"Dataset hint nao informado; a documentacao pode ficar incompleta sem introspecao real do schema."
		)

	if not table_name:
		table_name = "tabela_principal"
		if not table_path:
			table_path = f"{state.project_id}.dataset.tabela_principal"

	metadata["table_name"] = table_name
	metadata["table_path"] = table_path

	return {
		"doc_type": doc_type,
		"title": title,
		"objective": objective,
		"frequency": frequency,
		"table_name": table_name,
		"table_path": table_path,
		"metadata": metadata,
		"input_context": {
			"request_text": state.request_text,
			"project_id": state.project_id,
			"dataset_hint": dataset_name or state.dataset_hint or "",
			"explicit_table_ref": explicit_ref,
			"structured_blocks": blocks,
			"business_context": business_context_block,
		},
		"warnings": _dedupe(warnings),
	}


def fetch_real_schema(state: DocumentBuildState) -> dict[str, Any]:
	if state.error:
		return {}

	if not state.dataset_hint:
		return {
			"real_schema": {},
		}

	real_context = _build_real_dataset_context(
		project_id=state.project_id,
		dataset_hint=state.dataset_hint,
		request_text=state.request_text,
	)

	metadata = dict(state.metadata)
	metadata.update(real_context.get("metadata") or {})

	resolved_table_name = str(real_context.get("table_name") or "").strip()
	resolved_table_path = str(real_context.get("table_path") or "").strip()
	warnings = _dedupe(list(state.warnings) + list(real_context.get("warnings") or []))

	return {
		"metadata": metadata,
		"real_schema": {
			"dataset_ref": metadata.get("dataset_ref") or "",
			"selected_table": metadata.get("selected_table") or {},
			"tables": metadata.get("tables") or [],
		},
		"artifacts_context": {
			**(state.artifacts_context or {}),
			"real_schema": {
				"dataset_ref": metadata.get("dataset_ref") or "",
				"selected_table": metadata.get("selected_table") or {},
				"tables": metadata.get("tables") or [],
			},
		},
		"table_name": resolved_table_name or state.table_name,
		"table_path": resolved_table_path or state.table_path,
		"warnings": warnings,
	}


def fetch_dataplex_tags(state: DocumentBuildState) -> dict[str, Any]:
	if state.error:
		return {}

	table_path = (state.table_path or "").strip()
	if not table_path or table_path.count(".") != 2:
		return {
			"dataplex_context": {
				"aspect_types": [],
				"business_glossary": [],
				"entry_name": "",
				"warnings": [
					"Dataplex/Data Catalog nao consultado: table_path ausente ou invalido.",
				],
			},
		}

	context: dict[str, Any] = {
		"aspect_types": [],
		"business_glossary": [],
		"entry_name": "",
		"warnings": [],
	}
	warnings: list[str] = []

	try:
		project, dataset, table = table_path.split(".")
		credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)
		dataplex_client = dataplex_v1.CatalogServiceClient(credentials=credentials)

		# Busca a entry do BigQuery via Dataplex Catalog (substituto do Data Catalog depreciado)
		search_request = dataplex_v1.SearchEntriesRequest(
			name=f"projects/{project}/locations/global",
			query=f"{project}.{dataset}.{table} system=BIGQUERY",
			page_size=5,
		)
		results = list(dataplex_client.search_entries(request=search_request))

		matched_entry = None
		for result in results:
			if result.linked_resource and table in result.linked_resource:
				matched_entry = result.dataplex_entry
				break

		if matched_entry:
			context["entry_name"] = getattr(matched_entry, "name", "")
			for aspect_key, aspect in matched_entry.aspects.items():
				aspect_type_label = aspect_key.split("/")[-1] if "/" in aspect_key else aspect_key
				if aspect_type_label and aspect_type_label not in context["aspect_types"]:
					context["aspect_types"].append(aspect_type_label)

				try:
					data_dict = dict(aspect.data) if aspect.data else {}
					for v in data_dict.values():
						text = str(v).strip()
						if text and "gloss" in text.lower() and text not in context["business_glossary"]:
							context["business_glossary"].append(text)
				except Exception:
					pass

			if not context["aspect_types"]:
				warnings.append("Nenhum aspect type Dataplex encontrado para a tabela selecionada.")
	except NotFound:
		# Ausencia de entry no catalogo e um estado valido; nao gerar warning ao usuario.
		pass
	except Exception as exc:
		warnings.append(f"Falha ao consultar Dataplex Catalog: {exc}")

	return {
		"dataplex_context": {
			**context,
			"warnings": _dedupe(warnings),
		},
	}



def generate_document_structure(state: DocumentBuildState, llm: BaseChatModel) -> dict[str, Any]:
	if state.error:
		return {}

	real_context = _build_real_context_summary(state.metadata)
	dataplex_context = _build_dataplex_context_summary(state.dataplex_context)

	prompt = f"""
Contexto informado pelo usuario:
{state.request_text}

Parametros:
- Tipo de documento: {state.doc_type}
- Project ID: {state.project_id}
- Dataset hint: {state.dataset_hint or 'nao informado'}
- Nome da tabela alvo: {state.table_name or 'nao informado'}
- Caminho completo da tabela: {state.table_path or 'nao informado'}

Artefatos reais disponiveis:
{real_context}

Tags de governanca Dataplex/Data Catalog:
{dataplex_context}

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
		if _is_json_truncated(raw):
			raw = _attempt_json_repair(raw)
		payload = _parse_json_response(raw)

		sections = _normalize_sections(payload.get("sections"))
		data_dictionary = _normalize_data_dictionary(payload.get("data_dictionary"))
		typing_notes = _safe_list(payload.get("typing_notes"))
		pending_technical = _safe_list(payload.get("pending_technical"))
		acceptance_checklist = _clean_checklist(_safe_list(payload.get("acceptance_checklist")))
		governance = _normalize_governance(payload.get("governance"))
		governance = _merge_governance_with_dataplex(governance, state.dataplex_context)
		mermaid_diagram = _normalize_mermaid(payload.get("mermaid_diagram"))
		real_table_columns = _get_selected_table_columns(state.metadata, state.table_name)
		raw_next_steps = _safe_list(payload.get("next_steps"))

		enriched = _enrich_required_blocks(
			request_text=state.request_text,
			table_name=state.table_name,
			table_path=state.table_path,
			sections=sections,
			data_dictionary=data_dictionary,
			real_table_columns=real_table_columns,
			typing_notes=typing_notes,
			pending_technical=pending_technical,
			acceptance_checklist=acceptance_checklist,
			next_steps=raw_next_steps,
			doc_type=str(state.doc_type or ""),
			governance=governance,
			mermaid_diagram=mermaid_diagram,
		)

		dataplex_warnings = _safe_list(state.dataplex_context.get("warnings")) if isinstance(state.dataplex_context, dict) else []
		warnings = _dedupe(
			list(state.warnings)
			+ dataplex_warnings
			+ _safe_list(payload.get("warnings"))
		)
		sanitized_sections = _sanitize_sections(enriched["sections"])
		warnings_set = {w.strip().lower() for w in warnings}
		enriched["governance"]["notes"] = [
			note for note in enriched["governance"].get("notes", [])
			if note.strip().lower() not in warnings_set
		]
		sanitized_governance = _sanitize_governance(enriched["governance"])
		sanitized_warnings = _sanitize_text_list(warnings)

		return {
			"title": str(payload.get("title") or state.title),
			"doc_type": str(payload.get("doc_type") or state.doc_type),
			"summary": str(payload.get("summary") or "Documentacao gerada a partir do contexto informado."),
			"audience": str(payload.get("audience") or "Times de Dados, BI e Engenharia Analytics"),
			"objective": str(payload.get("objective") or state.objective),
			"frequency": str(payload.get("frequency") or state.frequency or "Batch diario"),
			"table_name": str(payload.get("table_name") or state.table_name),
			"table_path": str(payload.get("table_path") or state.table_path),
			"mermaid_diagram": enriched["mermaid_diagram"],
			"sections": sanitized_sections,
			"data_dictionary": enriched["data_dictionary"],
			"typing_notes": enriched["typing_notes"],
			"assumptions": _safe_list(payload.get("assumptions")),
			"risks": _safe_list(payload.get("risks")),
			"acceptance_checklist": enriched["acceptance_checklist"],
			"next_steps": enriched["next_steps"],
			"warnings": sanitized_warnings,
			"governance": sanitized_governance,
			"pending_technical": enriched["pending_technical"],
			"draft_context": {
				"llm_payload": payload,
				"raw_next_steps": raw_next_steps,
				"normalized_sections": sanitized_sections,
				"normalized_data_dictionary": enriched["data_dictionary"],
			},
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
		"## Header",
		f"**Tabela:** {state.table_name or 'nao informado'}",
		f"**Caminho GCP:** {state.table_path or 'nao informado'}",
		"",
		f"**Tipo:** {state.doc_type}",
		f"**Publico-alvo:** {state.audience or 'Times tecnicos'}",
		"",
		"## Diagrama de fluxo (Mermaid)",
		"```mermaid",
		state.mermaid_diagram or _default_mermaid(state.table_name or "tabela_atual"),
		"```",
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
		# Correção 3: garantir que content nunca seja renderizado como repr Python
		content = _safe_render_content(section.get("content", ""))
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

	lines.append("## 5. Atencao a tipagem")
	if state.typing_notes:
		for item in state.typing_notes:
			lines.append(f"- {item}")
	else:
		lines.append("- Validar colunas de ID para CAST explicito em JOINs quando houver divergencia de tipo.")
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

	if state.pending_technical:
		lines.append("## Pendencias tecnicas")
		for item in state.pending_technical:
			label = item if item.startswith("[PENDENCIA TECNICA]") else f"[PENDENCIA TECNICA] {item}"
			lines.append(f"- {label}")
		lines.append("")

	if state.warnings:
		_append_list_section(lines, "Observacoes", state.warnings)

	markdown = "\n".join(lines).strip() + "\n"
	quality_score = _estimate_quality_score(state)

	return {
		"markdown_document": markdown,
		"quality_score": quality_score,
		"output_context": {
			"markdown_document": markdown,
			"quality_score": quality_score,
		},
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
	if state.table_path:
		score += 4
	if state.mermaid_diagram:
		score += 8
	score += min(len(state.sections) * 8, 24)
	score += min(len(state.data_dictionary) * 3, 12)
	score += min(len(state.typing_notes) * 2, 8)
	score += min(len(state.acceptance_checklist) * 3, 12)
	score += min(len(state.next_steps) * 3, 12)
	score += min(len(state.risks) * 2, 8)
	if isinstance(state.governance, dict) and state.governance.get("aspect_types"):
		score += 6
	score += 4 if state.summary else 0
	score -= min(len(state.pending_technical) * 2, 10)
	score -= min(len(state.warnings) * 3, 12)
	return max(0, min(100, score))


def _extract_message_content(response: Any) -> str:
	if hasattr(response, "content"):
		return str(response.content).strip()
	return str(response).strip()


def _is_json_truncated(raw: str) -> bool:
	"""Detecta se a resposta foi cortada antes do fechamento do JSON."""
	stripped = raw.strip()
	return bool(stripped) and stripped[-1] not in ("}", "]")


def _attempt_json_repair(raw: str) -> str:
	"""Tenta fechar um JSON truncado adicionando fechamentos pendentes."""
	open_braces = raw.count("{") - raw.count("}")
	open_brackets = raw.count("[") - raw.count("]")

	repaired = raw.rstrip().rstrip(",")
	repaired += "]" * max(open_brackets, 0)
	repaired += "}" * max(open_braces, 0)
	return repaired


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
			"table_name": "tabela_principal",
			"table_path": "<project_id>.dataset.tabela_principal",
			"mermaid_diagram": "graph TD\n  A[Origem] --> B[Processamento BigQuery/Dataform]\n  B --> C[Tabela Atual]",
			"sections": [
				{
					"title": "Conteudo bruto",
					"content": cleaned,
				}
			],
			"data_dictionary": [],
			"typing_notes": [],
			"governance": {
				"aspect_types": [],
				"readers": [],
				"notes": [],
			},
			"pending_technical": [
				"[PENDENCIA TECNICA] Resposta fora do padrao JSON, revisar metadados de origem.",
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

		# Filtrar pelo título exato
		if title.lower() in _SUMMARY_SECTION_TITLES:
			continue

		# Filtrar variações com prefixo emoji/pontuação/números
		title_key = re.sub(r"[^a-z0-9à-ÿ\s]", " ", title.lower())
		title_key = re.sub(r"\s+", " ", title_key).strip()
		if title_key in _SUMMARY_SECTION_TITLES:
			continue

		# Descartar seções cujo conteúdo é apenas índice de passos terminados em ":"
		lines = [l.strip() for l in content.splitlines() if l.strip()]
		if lines and all(re.match(r"^\d+[\.\-—]\s*.+:$", l) for l in lines):
			continue

		TITULOS_PARA_IGNORAR = {
			"passos operacionais", "sumário", "sumario",
			"índice", "indice", "resumo dos passos", "lista de passos",
		}
		if title.lower() in TITULOS_PARA_IGNORAR:
			continue

		linhas = [l.strip() for l in content.splitlines() if l.strip()]
		if linhas and all(re.match(r"^\d+[\.\-]\s*.+:$", l) for l in linhas):
			continue

		if not title and not content:
			continue

		sections.append({"title": title or "Seção", "content": content or "Sem conteúdo informado."})
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


def _normalize_mermaid(value: Any) -> str:
	text = str(value or "").strip()
	if not text:
		return ""
	if "graph " in text or "sequenceDiagram" in text:
		return text
	return ""


def _normalize_governance(value: Any) -> dict[str, list[str]]:
	if not isinstance(value, dict):
		return {"aspect_types": [], "readers": [], "notes": []}

	def _filter_meaningful(items: Any) -> list[str]:
		return [
			i for i in _safe_list(items)
			if _is_meaningful_governance_item(i)
		]

	raw_readers = _filter_meaningful(value.get("readers"))
	valid_readers = [r for r in raw_readers if not _is_table_path(r)]

	return {
		"aspect_types": _filter_meaningful(value.get("aspect_types")),
		"readers": valid_readers,
		"notes": _safe_list(value.get("notes")),
	}


def _enrich_required_blocks(
	request_text: str,
	table_name: str,
	table_path: str,
	sections: list[dict[str, str]],
	data_dictionary: list[dict[str, str]],
	real_table_columns: list[dict[str, str]],
	typing_notes: list[str],
	pending_technical: list[str],
	acceptance_checklist: list[str],
	next_steps: list[str],
	governance: dict[str, list[str]],
	mermaid_diagram: str,
	doc_type: str = "",
) -> dict[str, Any]:
	normalized = request_text.lower()
	next_steps = _clean_next_steps(next_steps)
	acceptance_checklist = _clean_checklist(acceptance_checklist)

	if not sections:
		sections = [
			{
				"title": "Arquitetura da pipeline",
				"content": (
					"Fluxo de ingestao e enriquecimento com camadas de validacao, regras de negocio e publicacao para consumo analitico."
				),
			}
		]

	sections = _remove_incomplete_runbook_summary_sections(sections)

	# Correção 1: remover seção de dicionário de dados das sections LLM quando schema real existe
	has_real_schema = bool(real_table_columns)
	if has_real_schema:
		sections = [
			s for s in sections
			if not re.search(
				r'dicion[aá]rio\s*de\s*dados?|data\s*dict(ionary)?',
				s.get('title', ''),
				re.IGNORECASE,
			)
		]

	if real_table_columns:
		real_columns_map = {str(col.get("name") or "").strip().lower(): col for col in real_table_columns}
		filtered_rows: list[dict[str, str]] = []
		for row in data_dictionary:
			column = str(row.get("column") or "").strip()
			if not column:
				continue
			if column.lower() not in real_columns_map:
				pending_technical.append(
					f"[PENDENCIA TECNICA] Coluna nao encontrada no schema real: {column}."
				)
				continue

			real_col = real_columns_map[column.lower()]
			mode = str(real_col.get("mode") or "").strip()
			type_real = str(real_col.get("type") or row.get("type") or "STRING")
			type_display = _format_type_with_mode(type_real, mode)
			# Correção 2: usar regra inferida quando LLM não forneceu uma específica
			business_rule = str(row.get("business_rule") or "").strip()
			if not business_rule or business_rule.lower() in {"sem regra de negocio.", "-", ""}:
				business_rule = _infer_business_rule(column, type_real, mode)
			filtered_rows.append(
				{
					"column": column,
					"type": type_display,
					"description": str(row.get("description") or real_col.get("description") or "Sem descricao."),
					"business_rule": business_rule,
				}
			)

		if filtered_rows:
			data_dictionary = filtered_rows
		else:
			data_dictionary = _build_data_dictionary_from_schema(real_table_columns)
	elif not data_dictionary:
		pending_technical.append(
			"[PENDENCIA TECNICA] Schema real indisponivel para montar data dictionary completo."
		)

	for row in data_dictionary:
		column = (row.get("column") or "").strip()
		type_name = (row.get("type") or "").strip()
		description = (row.get("description") or "").strip()
		if not type_name:
			pending_technical.append(
				f"[PENDENCIA TECNICA] Tipo primitivo ausente na coluna {column or 'desconhecida'}."
			)
		if not description or description.lower().startswith("sem descricao"):
			pending_technical.append(
				f"[PENDENCIA TECNICA] Descricao ausente na coluna {column or 'desconhecida'}."
			)

	id_rows = [row for row in data_dictionary if "id" in (row.get("column") or "").lower()]
	if id_rows and not typing_notes:
		for row in id_rows:
			typing_notes.append(
				f"Coluna {row.get('column', 'id')}: validar compatibilidade de tipo em JOIN e aplicar CAST explicito quando necessario."
			)

	if not mermaid_diagram:
		mermaid_diagram = _default_mermaid(table_name or "tabela_atual")

	if table_path and not any("table_path" in note for note in governance.get("notes", [])):
		governance.setdefault("notes", []).append(f"table_path: {table_path}")

	explicit_checks = _extract_explicit_dq_checks(request_text)
	if explicit_checks:
		acceptance_checklist = _clean_checklist(_dedupe(explicit_checks + acceptance_checklist))
	else:
		mandatory_checks = _build_dynamic_dq_checks(data_dictionary, real_table_columns)
		for check in mandatory_checks:
			if check not in acceptance_checklist:
				acceptance_checklist.append(check)
		acceptance_checklist = _clean_checklist(acceptance_checklist)

	if not governance.get("aspect_types"):
		governance["aspect_types"] = [
			"schema_contract",
			"data_quality_profile",
			"data_owner",
		]
	# Correção 4: remover readers que sejam caminhos de tabela gerados erroneamente pela LLM
	governance["readers"] = [r for r in _safe_list(governance.get("readers")) if not _is_table_path(r)]
	if not governance.get("readers"):
		governance["readers"] = ["Nao configurado — consultar responsavel pelo dominio de dados."]
	if not governance.get("notes"):
		governance["notes"] = [
			"Habilitar monitoramento de desvio de esquema no Dataplex para alertas proativos.",
		]

	explicit_steps = _extract_explicit_runbook_steps(request_text)
	if explicit_steps:
		next_steps = _clean_next_steps(_dedupe(explicit_steps + next_steps))
		has_runbook_section = any(
			re.search(r"passos?|etapas?|runbook|procedimento", str(s.get("title") or ""), re.IGNORECASE)
			for s in sections
		)
		if not has_runbook_section:
			sections.append(
				{
					"title": "Passos operacionais",
					"content": "\n".join(f"{idx + 1}. {step}" for idx, step in enumerate(explicit_steps)),
				}
			)

	if not next_steps:
		doc_type_lower = (doc_type or "").lower()
		if "runbook" in doc_type_lower:
			next_steps = [
				"Realizar as verificações diárias de forma autônoma.",
				"Em caso de anomalias, escalar para o lead da equipe.",
				"Revisar e atualizar este runbook conforme o pipeline evolui.",
			]
		else:
			next_steps = [
				"Configurar alerta de schema drift no Dataplex para a tabela.",
				"Publicar runbook de tratamento para quebra de contrato.",
				"Definir dono de dados e SLA de correção para incidentes de DQ.",
			]

	return {
		"sections": sections,
		"data_dictionary": data_dictionary,
		"typing_notes": _dedupe(typing_notes),
		"pending_technical": _dedupe(pending_technical),
		"acceptance_checklist": acceptance_checklist,
		"next_steps": next_steps,
		"governance": governance,
		"mermaid_diagram": mermaid_diagram,
	}


def _build_real_dataset_context(
	project_id: str,
	dataset_hint: str,
	request_text: str,
) -> dict[str, Any]:
	metadata: dict[str, Any] = {
		"project_id": project_id,
		"dataset_hint": dataset_hint,
	}
	warnings: list[str] = []

	try:
		schema_catalog = get_dataset_tables_schema(project_id, dataset_hint, max_tables=30, max_columns=50)
		tables = schema_catalog.get("tables") or []
		metadata.update(
			{
				"dataset_ref": schema_catalog.get("dataset_ref") or "",
				"tables": tables,
			}
		)

		if not tables:
			warnings.append("Dataset validado sem tabelas visiveis para introspecao de schema.")
			return {"metadata": metadata, "warnings": warnings, "table_name": "", "table_path": ""}

		selected_table = _select_table_from_catalog(request_text, tables)
		if not selected_table:
			selected_table = tables[0]
			warnings.append(
				"Tabela alvo nao identificada no texto; usando a primeira tabela do dataset para documentacao base."
			)

		table_name = str(selected_table.get("table_id") or "").strip()
		table_path = str(selected_table.get("full_name") or "").strip()
		metadata["selected_table"] = selected_table

		return {
			"metadata": metadata,
			"warnings": warnings,
			"table_name": table_name,
			"table_path": table_path,
		}
	except Exception as exc:
		warnings.append(f"Nao foi possivel carregar schema real do dataset: {exc}")
		return {"metadata": metadata, "warnings": warnings, "table_name": "", "table_path": ""}


def _select_table_from_catalog(request_text: str, tables: list[dict[str, Any]]) -> dict[str, Any] | None:
	if not tables:
		return None

	normalized = request_text.lower()
	explicit = _extract_explicit_table_reference(request_text)
	full_ref = str(explicit.get("full_name") or "").lower()
	if full_ref:
		for item in tables:
			if str(item.get("full_name") or "").lower() == full_ref:
				return item

	explicit_table = str(explicit.get("table") or "").lower()
	if explicit_table:
		for item in tables:
			if str(item.get("table_id") or "").lower() == explicit_table:
				return item

	for item in tables:
		table_id = str(item.get("table_id") or "").lower()
		if table_id and table_id in normalized:
			return item

	return None


def _extract_explicit_table_reference(text: str) -> dict[str, str]:
	normalized = (text or "").strip()
	if not normalized:
		return {"project": "", "dataset": "", "table": "", "full_name": ""}

	full_match = re.search(r"`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?", normalized)
	if full_match:
		project, dataset, table = full_match.groups()
		return {
			"project": project,
			"dataset": dataset,
			"table": table,
			"full_name": f"{project}.{dataset}.{table}",
		}

	ds_table_match = re.search(r"`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?", normalized)
	if ds_table_match:
		dataset, table = ds_table_match.groups()
		return {
			"project": "",
			"dataset": dataset,
			"table": table,
			"full_name": f"{dataset}.{table}",
		}

	return {"project": "", "dataset": "", "table": "", "full_name": ""}


def _extract_structured_input_blocks(text: str) -> dict[str, str]:
	raw = (text or "").strip()
	if not raw:
		return {"tabela": "", "objetivo": "", "contexto_negocio": "", "tipo_doc": ""}

	patterns = {
		"tabela": r"\[TABELA\]\s*([\s\S]*?)(?=\n\s*\[[^\]]+\]|$)",
		"objetivo": r"\[OBJETIVO\]\s*([\s\S]*?)(?=\n\s*\[[^\]]+\]|$)",
		"contexto_negocio": r"\[(?:CONTEXTO\s+DE\s+NEG[ÓO]CIO|CONTEXTO\s+DE\s+NEGOCIO)\]\s*([\s\S]*?)(?=\n\s*\[[^\]]+\]|$)",
		"tipo_doc": r"\[TIPO\s+DE\s+DOC\]\s*([\s\S]*?)(?=\n\s*\[[^\]]+\]|$)",
	}

	result: dict[str, str] = {}
	for key, pattern in patterns.items():
		match = re.search(pattern, raw, flags=re.IGNORECASE)
		if not match:
			result[key] = ""
			continue
		value = match.group(1).strip()
		first_line = value.splitlines()[0].strip() if value else ""
		result[key] = first_line if key in {"tabela", "tipo_doc"} else value

	return result


def _build_real_context_summary(metadata: dict[str, Any]) -> str:
	if not isinstance(metadata, dict):
		return "Sem artefatos tecnicos reais no contexto."

	tables = metadata.get("tables") if isinstance(metadata.get("tables"), list) else []
	if not tables:
		return "Sem artefatos tecnicos reais no contexto."

	selected = metadata.get("selected_table") if isinstance(metadata.get("selected_table"), dict) else {}
	selected_name = str(selected.get("full_name") or "")
	lines = [
		f"Dataset: {metadata.get('dataset_ref') or metadata.get('dataset_hint')}",
		f"Total de tabelas catalogadas: {len(tables)}",
	]

	if selected_name:
		lines.append(f"Tabela selecionada para documentacao: {selected_name}")

	for table in tables[:8]:
		table_ref = str(table.get("full_name") or "")
		cols = table.get("columns") if isinstance(table.get("columns"), list) else []
		col_preview = ", ".join(
			f"{c.get('name')}:{c.get('type')}"
			for c in cols[:12]
			if isinstance(c, dict)
		)
		lines.append(f"- {table_ref} | colunas: {col_preview or '(sem colunas carregadas)'}")

	if len(tables) > 8:
		lines.append("- ...")

	return "\n".join(lines)


def _build_dataplex_context_summary(dataplex_context: dict[str, Any]) -> str:
	if not isinstance(dataplex_context, dict) or not dataplex_context:
		return "Sem contexto Dataplex/Data Catalog disponivel."

	aspects = dataplex_context.get("aspect_types") if isinstance(dataplex_context.get("aspect_types"), list) else []
	glossary = dataplex_context.get("business_glossary") if isinstance(dataplex_context.get("business_glossary"), list) else []
	entry_name = str(dataplex_context.get("entry_name") or "").strip()

	lines = [
		f"Entry: {entry_name or 'nao encontrado'}",
		"Aspect types: " + (", ".join(aspects) if aspects else "nenhum"),
	]

	if glossary:
		lines.append("Glossario: " + ", ".join(glossary[:10]))

	return "\n".join(lines)


def _merge_governance_with_dataplex(
	governance: dict[str, list[str]],
	dataplex_context: dict[str, Any],
) -> dict[str, list[str]]:
	merged = {
		"aspect_types": _safe_list(governance.get("aspect_types")),
		"readers": _safe_list(governance.get("readers")),
		"notes": _safe_list(governance.get("notes")),
	}

	if not isinstance(dataplex_context, dict):
		return merged

	for aspect in _safe_list(dataplex_context.get("aspect_types")):
		if aspect not in merged["aspect_types"]:
			merged["aspect_types"].append(aspect)

	for term in _safe_list(dataplex_context.get("business_glossary")):
		note = f"Glossario: {term}"
		if note not in merged["notes"]:
			merged["notes"].append(note)

	entry_name = str(dataplex_context.get("entry_name") or "").strip()
	if entry_name:
		note = f"Data Catalog entry: {entry_name}"
		if note not in merged["notes"]:
			merged["notes"].append(note)

	return merged


def _get_selected_table_columns(metadata: dict[str, Any], table_name: str) -> list[dict[str, str]]:
	if not isinstance(metadata, dict):
		return []

	selected = metadata.get("selected_table")
	if isinstance(selected, dict) and isinstance(selected.get("columns"), list):
		return [col for col in selected.get("columns", []) if isinstance(col, dict)]

	tables = metadata.get("tables") if isinstance(metadata.get("tables"), list) else []
	for table in tables:
		if not isinstance(table, dict):
			continue
		if str(table.get("table_id") or "").strip().lower() == (table_name or "").strip().lower():
			cols = table.get("columns") if isinstance(table.get("columns"), list) else []
			return [col for col in cols if isinstance(col, dict)]

	return []


def _build_data_dictionary_from_schema(columns: list[dict[str, str]]) -> list[dict[str, str]]:
	rows: list[dict[str, str]] = []
	for col in columns:
		name = str(col.get("name") or "").strip()
		type_name = str(col.get("type") or "STRING").strip() or "STRING"
		mode = str(col.get("mode") or "NULLABLE").strip() or "NULLABLE"
		description = str(col.get("description") or "").strip() or "Sem descricao."
		if not name:
			continue
		# Correção 2: inferir regra de negócio real ao invés de placeholder genérico
		business_rule = _infer_business_rule(name, type_name, mode)
		rows.append(
			{
				"column": name,
				"type": _format_type_with_mode(type_name, mode),
				"description": description,
				"business_rule": business_rule,
			}
		)
	return rows


def _format_type_with_mode(type_name: str, mode: str) -> str:
	base = str(type_name or "STRING").strip() or "STRING"
	mode_normalized = str(mode or "").strip().upper()
	if mode_normalized in {"NULLABLE", "REQUIRED", "REPEATED"}:
		return f"{base} ({mode_normalized})"
	return base


def _extract_explicit_dq_checks(request_text: str) -> list[str]:
	text = str(request_text or "")
	if not text.strip():
		return []

	checks: list[str] = []
	for line in text.splitlines():
		clean = line.strip()
		if not clean:
			continue
		if re.match(r"^[-*]\s+", clean):
			item = re.sub(r"^[-*]\s+", "", clean).strip()
			if re.search(r"dq|qualidade|great expectations|dataform|volume|nulo|unicidade|schema", item, re.IGNORECASE):
				checks.append(item)
		elif re.match(r"^\d+[\.)]\s+", clean):
			item = re.sub(r"^\d+[\.)]\s+", "", clean).strip()
			if re.search(r"dq|qualidade|great expectations|dataform|volume|nulo|unicidade|schema", item, re.IGNORECASE):
				checks.append(item)

	return _dedupe(checks)


def _extract_explicit_runbook_steps(request_text: str) -> list[str]:
	text = str(request_text or "")
	if not text.strip():
		return []

	steps: list[str] = []
	for line in text.splitlines():
		clean = line.strip()
		if not clean:
			continue
		if re.match(r"^(passo\s*\d+|\d+[\.)])\s*", clean, re.IGNORECASE):
			item = re.sub(r"^(passo\s*\d+\s*[:\-]?|\d+[\.)]\s*)", "", clean, flags=re.IGNORECASE).strip()
			if item:
				steps.append(item)

	return _dedupe(steps)


def _clean_next_steps(items: list[str]) -> list[str]:
	"""Remove itens de next_steps incompletos ou contraditorios."""
	skip_markers = {
		"nenhum proximo passo",
		"nenhum próximo passo",
		"nao definido",
		"não definido",
	}

	cleaned: list[str] = []
	for item in items:
		value = str(item or "").strip()
		if not value:
			continue
		if value.endswith(":"):
			continue
		if _looks_like_summary_index_line(value):
			continue
		value_lower = value.lower()
		if any(marker in value_lower for marker in skip_markers):
			continue
		cleaned.append(value)

	return _dedupe(cleaned)


def _clean_checklist(items: list[str]) -> list[str]:
	"""Remove itens incompletos de checklist e placeholders de indice."""
	cleaned: list[str] = []
	for item in items:
		value = str(item or "").strip()
		if not value:
			continue
		if value.endswith(":"):
			continue
		if _looks_like_summary_index_line(value):
			continue
		value_lower = value.lower()
		if value_lower in _EMPTY_MARKERS or value_lower.startswith("nenhum"):
			continue
		cleaned.append(value)
	return _dedupe(cleaned)


def _remove_incomplete_runbook_summary_sections(sections: list[dict[str, str]]) -> list[dict[str, str]]:
	"""Remove secoes de sumario/indice com apenas titulos de passos sem conteudo."""
	result: list[dict[str, str]] = []
	for section in sections:
		title = str(section.get("title") or "").strip()
		content = str(section.get("content") or "").strip()
		title_is_summary = title.strip().lower() in _SUMMARY_SECTION_TITLES
		if not title_is_summary:
			title_is_summary = bool(re.search(r"sum[aá]rio|resumo|[ií]ndice|passos operacionais", title, re.IGNORECASE))
		if not title_is_summary:
			result.append(section)
			continue

		if re.search(r"o que fazer|como fazer|o que verificar", content, re.IGNORECASE):
			result.append(section)
			continue

		# Titulo de sumario sem conteudo operacional — descartar

	return result


def _looks_like_step_heading(line: str) -> bool:
	return bool(
		re.match(r"^(passo\s*\d+|\d+[\.)])\s*(?:[-–—]\s*)?.+:\s*$", str(line or "").strip(), re.IGNORECASE)
	)


def _looks_like_summary_index_line(line: str) -> bool:
	value = str(line or "").strip()
	if not value:
		return False
	if _looks_like_step_heading(value):
		return True
	return bool(re.match(r"^(passo\s*\d+|\d+[\.)])\s+.+$", value, re.IGNORECASE))


def _sanitize_sections(sections: list[dict[str, str]]) -> list[dict[str, str]]:
	result: list[dict[str, str]] = []
	for section in sections:
		title = str(section.get("title") or "").strip()
		content = str(section.get("content") or "")
		clean_lines = [ln for ln in content.splitlines() if not _is_prompt_instruction_leak(ln)]
		clean_content = "\n".join(clean_lines).strip()
		if not title and not clean_content:
			continue
		result.append({"title": title or "Secao", "content": clean_content or "Sem conteudo informado."})
	return result


def _sanitize_governance(governance: dict[str, list[str]]) -> dict[str, list[str]]:
	if not isinstance(governance, dict):
		return {"aspect_types": [], "readers": [], "notes": []}
	return {
		"aspect_types": _sanitize_text_list(_safe_list(governance.get("aspect_types"))),
		"readers": _sanitize_text_list(_safe_list(governance.get("readers"))),
		"notes": _sanitize_text_list(_safe_list(governance.get("notes"))),
	}


def _sanitize_text_list(values: list[str]) -> list[str]:
	return [v for v in _dedupe(values) if not _is_prompt_instruction_leak(v)]


def _is_meaningful_governance_item(value: str) -> bool:
	item = str(value or "").strip()
	if not item:
		return False
	item_lower = item.lower()
	if item_lower in _EMPTY_MARKERS:
		return False
	if item_lower.startswith("nenhum"):
		return False
	if item_lower.startswith("não há"):
		return False
	if item_lower.startswith("nao ha"):
		return False
	if len(item) >= 80:
		return False
	return True


def _is_prompt_instruction_leak(text: str) -> bool:
	normalized = str(text or "").strip().lower()
	if not normalized:
		return False
	patterns = [
		r"se houver schema real, use apenas colunas e tipos do schema fornecido",
		r"nao invente campos fora do catalogo",
		r"não invente campos fora do catálogo",
		r"registre em pending_technical",
		r"instrucoes criticas",
		r"instrucoes internas",
		r"regras internas",
	]
	return any(re.search(p, normalized) for p in patterns)


def _build_dynamic_dq_checks(
	data_dictionary: list[dict[str, str]],
	real_table_columns: list[dict[str, str]],
) -> list[str]:
	checks: list[str] = []
	col_names = [str(row.get("column") or "").strip() for row in data_dictionary]
	col_names_lower = [name.lower() for name in col_names if name]

	id_candidates = [name for name in col_names if name.lower().endswith("_id") or name.lower() == "id"]
	if id_candidates:
		checks.append(f"A coluna {id_candidates[0]} nao possui duplicidade para a granularidade esperada?")

	required_cols = [
		str(col.get("name") or "").strip()
		for col in real_table_columns
		if str(col.get("mode") or "").strip().upper() == "REQUIRED"
	]
	if required_cols:
		checks.append(
			"Nao existem nulos nas colunas obrigatorias do schema: "
			+ ", ".join(required_cols[:5])
			+ (" ..." if len(required_cols) > 5 else "")
		)

	numeric_cols = [
		str(col.get("name") or "").strip()
		for col in real_table_columns
		if str(col.get("type") or "").strip().upper() in {"INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC"}
	]
	if numeric_cols:
		checks.append(
			"Campos numericos criticos estao dentro de faixas validas e sem outliers extremos?"
		)

	if any("data" in name or "dt_" in name or name.endswith("_dt") for name in col_names_lower):
		checks.append("Existe controle de atualizacao/recencia para colunas de data da tabela?")

	if not checks:
		checks.append("A tabela atende criterios minimos de completude, unicidade e consistencia de tipos?")

	return checks


def _default_mermaid(table_name: str) -> str:
	name = table_name or "tabela_atual"
	return (
		"graph TD\n"
		"  A[Origem - Bronze]:::bronze --> B[Transformacao - BigQuery/Dataform]:::silver\n"
		f"  B --> C[Destino - {name}]:::gold\n"
		"  classDef bronze fill:#fdf2e0,stroke:#c97526,color:#7a4b1f;\n"
		"  classDef silver fill:#edf2f7,stroke:#64748b,color:#334155;\n"
		"  classDef gold fill:#fff7db,stroke:#b78a17,color:#6b4e00;"
	)


def _safe_render_content(content: Any) -> str:
	"""Converte content de qualquer tipo em string renderizável — evita repr() de list/dict."""
	if content is None:
		return ""
	if isinstance(content, str):
		return content.strip()
	if isinstance(content, list):
		return "\n".join(f"- {item}" for item in content if item)
	if isinstance(content, dict):
		return "\n".join(f"**{k}:** {v}" for k, v in content.items())
	return str(content).strip()


def _is_table_path(value: str) -> bool:
	"""Retorna True se o valor parecer um caminho project.dataset.table."""
	return bool(re.match(r'^[a-z0-9_\-]+\.[a-z0-9_\-]+\.[a-z0-9_\-]+$', value.strip(), re.IGNORECASE))


def _infer_business_rule(name: str, type_name: str, mode: str) -> str:
	"""Infere regra de negócio baseada no nome da coluna, tipo e modo."""
	n = name.lower()
	t = type_name.upper()
	rules: list[str] = []

	if mode.upper() == "REQUIRED":
		rules.append("Obrigatório (REQUIRED) — não aceita nulos.")

	if n.endswith("_id") or n == "id":
		rules.append("Chave identificadora — validar unicidade e aplicar CAST explícito em JOINs com tipos divergentes.")
	elif any(kw in n for kw in ("valor", "value", "preco", "custo", "receita", "margem", "lucro", "roi")):
		rules.append("Campo financeiro — não pode ser nulo em registros válidos; verificar outliers e valores negativos inesperados.")
	elif any(kw in n for kw in ("data_", "dt_", "_dt", "_date", "_at", "_time")) or n.startswith("dt"):
		rules.append("Campo de data/timestamp — monitorar recência e SLA de atualização; validar formato.")
	elif any(kw in n for kw in ("canal", "tipo", "segmento", "categoria", "modalidade")):
		rules.append("Campo categórico — validar valores contra o domínio permitido.")
	elif any(kw in n for kw in ("status", "estado", "flag", "ativo", "situacao")):
		rules.append("Campo de status — verificar cobertura dos estados válidos e ausência de valores não mapeados.")
	elif any(kw in n for kw in ("nome", "name", "descricao", "description", "label")):
		rules.append("Campo descritivo — verificar completude e ausência de strings vazias ou nulas.")

	if not rules:
		if any(kw in t for kw in ("FLOAT", "NUMERIC", "BIGNUMERIC", "INT")):
			rules.append("Campo numérico — verificar precisão e ausência de valores fora da faixa esperada.")
		elif "BOOL" in t:
			rules.append("Campo booleano — valores esperados: TRUE/FALSE. Verificar ausência de nulos em registros ativos.")
		elif "TIMESTAMP" in t or "DATE" in t or "TIME" in t:
			rules.append("Campo temporal — monitorar recência e consistência com SLA de atualização.")

	return " ".join(rules) if rules else "Sem regra de negócio específica — validar com o responsável pelo domínio."


def _dedupe(items: list[str]) -> list[str]:
	seen: set[str] = set()
	result: list[str] = []
	for item in items:
		key = item.strip()
		if not key or key in seen:
			continue
		seen.add(key)
		result.append(key)
	return result
