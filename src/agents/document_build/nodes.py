from __future__ import annotations

import json
import re
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from src.agents.document_build.prompts import DOCUMENT_BUILD_SYSTEM_PROMPT
from src.agents.document_build.state import DocumentBuildState
from src.shared.tools.bigquery import get_dataset_tables_schema


def parse_document_request(state: DocumentBuildState) -> dict[str, Any]:
	text = (state.request_text or "").strip()
	if not text:
		return {"error": "Descreva o contexto para gerar a documentacao."}

	normalized = text.lower()
	doc_type = "documentacao_funcional"
	if any(word in normalized for word in ["data dictionary", "dicionario", "dicionario de dados"]):
		doc_type = "data_dictionary"
	elif any(word in normalized for word in ["contract", "data contract", "pipeline", "sla", "especificacao de pipeline"]):
		doc_type = "pipeline_data_contract"
	elif any(word in normalized for word in ["schema contract", "qualidade", "data quality", "dq", "great expectations"]):
		doc_type = "schema_contract"
	if any(word in normalized for word in ["runbook", "operacao", "incidente", "suporte"]):
		doc_type = "runbook_operacional"

	table_name = _infer_table_name(text) or ""
	dataset_name = (state.dataset_hint or "").strip()
	table_path = f"{state.project_id}.{dataset_name}.{table_name}" if (table_name and dataset_name) else ""

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
	metadata: dict[str, Any] = {
		"project_id": state.project_id,
		"dataset_hint": state.dataset_hint or "nao informado",
		"table_name": table_name,
		"table_path": table_path,
	}
	warnings: list[str] = []

	if state.dataset_hint:
		real_context = _build_real_dataset_context(
			project_id=state.project_id,
			dataset_hint=state.dataset_hint,
			request_text=text,
		)
		metadata.update(real_context.get("metadata") or {})
		warnings.extend(real_context.get("warnings") or [])

		resolved_table_name = str(real_context.get("table_name") or "").strip()
		resolved_table_path = str(real_context.get("table_path") or "").strip()
		if resolved_table_name:
			table_name = resolved_table_name
		if resolved_table_path:
			table_path = resolved_table_path
	else:
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
		"warnings": _dedupe(warnings),
	}


def generate_document_structure(state: DocumentBuildState, llm: BaseChatModel) -> dict[str, Any]:
	if state.error:
		return {}

	real_context = _build_real_context_summary(state.metadata)

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

Instrucoes criticas:
- Se houver schema real, use APENAS colunas e tipos do schema fornecido.
- Nao invente campos fora do catalogo.
- Se alguma informacao essencial nao existir no schema, registre em pending_technical.

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
		typing_notes = _safe_list(payload.get("typing_notes"))
		pending_technical = _safe_list(payload.get("pending_technical"))
		acceptance_checklist = _safe_list(payload.get("acceptance_checklist"))
		governance = _normalize_governance(payload.get("governance"))
		mermaid_diagram = _normalize_mermaid(payload.get("mermaid_diagram"))
		real_table_columns = _get_selected_table_columns(state.metadata, state.table_name)

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
			next_steps=_safe_list(payload.get("next_steps")),
			governance=governance,
			mermaid_diagram=mermaid_diagram,
		)

		warnings = _dedupe(list(state.warnings) + _safe_list(payload.get("warnings")))

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
			"sections": enriched["sections"],
			"data_dictionary": enriched["data_dictionary"],
			"typing_notes": enriched["typing_notes"],
			"assumptions": _safe_list(payload.get("assumptions")),
			"risks": _safe_list(payload.get("risks")),
			"acceptance_checklist": enriched["acceptance_checklist"],
			"next_steps": enriched["next_steps"],
			"warnings": warnings,
			"governance": enriched["governance"],
			"pending_technical": enriched["pending_technical"],
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
		f"**Project ID:** {state.metadata.get('project_id', state.project_id)}",
		f"**Dataset hint:** {state.metadata.get('dataset_hint', state.dataset_hint or 'nao informado')}",
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

	return {
		"aspect_types": _safe_list(value.get("aspect_types")),
		"readers": _safe_list(value.get("readers")),
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
			filtered_rows.append(
				{
					"column": column,
					"type": str(real_col.get("type") or row.get("type") or "STRING"),
					"description": str(row.get("description") or real_col.get("description") or "Sem descricao."),
					"business_rule": str(row.get("business_rule") or "Sem regra de negocio."),
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

	mandatory_checks = _build_dynamic_dq_checks(data_dictionary, real_table_columns)
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
	full_ref_match = re.search(r"([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)", normalized)
	if full_ref_match:
		full_ref = full_ref_match.group(1)
		for item in tables:
			if str(item.get("full_name") or "").lower() == full_ref:
				return item

	inferred_name = _infer_table_name(request_text).lower()
	if inferred_name:
		for item in tables:
			if str(item.get("table_id") or "").lower() == inferred_name:
				return item

	for item in tables:
		table_id = str(item.get("table_id") or "").lower()
		if table_id and table_id in normalized:
			return item

	return None


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

		business_rule = "Campo mapeado a partir do schema real."
		if mode.upper() == "REQUIRED":
			business_rule = "Campo obrigatorio no schema (REQUIRED)."

		rows.append(
			{
				"column": name,
				"type": f"{type_name} ({mode})",
				"description": description,
				"business_rule": business_rule,
			}
		)
	return rows


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


def _infer_table_name(text: str) -> str:
	match = re.search(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", text)
	if not match:
		return ""
	# Prioriza nomes com underscore tipicos de tabela
	candidates = re.findall(r"\b([a-z][a-z0-9_]{3,})\b", text)
	for candidate in candidates:
		if "_" in candidate:
			return candidate
	return ""


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
