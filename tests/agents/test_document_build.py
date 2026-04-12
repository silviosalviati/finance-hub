from src.agents.document_build import DocumentBuildAgent
from src.agents.document_build.nodes import (
    _extract_explicit_table_reference,
    _merge_governance_with_dataplex,
    parse_document_request,
    finalize_document_markdown,
)
from src.agents.document_build.state import DocumentBuildState


def test_document_build_agent_metadata():
    agent = DocumentBuildAgent()
    assert agent.agent_id == "document_build"
    assert agent.display_name == "Document Build"


def test_document_build_runtime_info():
    agent = DocumentBuildAgent()
    info = agent.runtime_info()

    assert info["provider"] == "shared"


def test_document_build_markdown_has_required_sections():
    state = DocumentBuildState(
        request_text="Gerar doc da pipeline de perfil comportamental.",
        project_id="silviosalviati",
        dataset_hint="clientes",
        title="Documentacao Tecnica - Perfil Comportamental",
        doc_type="especificacao_tecnica",
        audience="Dados e BI",
        objective="Centralizar indicadores de risco.",
        frequency="Batch diario",
        table_name="perfil_comportamental_clientes",
        table_path="silviosalviati.clientes.perfil_comportamental_clientes",
        mermaid_diagram="graph TD\nA[Origem] --> B[Processamento] --> C[Destino]",
        summary="Resumo do processo.",
        sections=[{"title": "Arquitetura", "content": "Fluxo bronze/silver/gold."}],
        data_dictionary=[
            {
                "column": "cliente_id",
                "type": "INTEGER",
                "description": "Identificador.",
                "business_rule": "Casting para INTEGER em joins.",
            }
        ],
        acceptance_checklist=["O cliente_id e unico (Primary Key)?"],
        governance={
            "aspect_types": ["schema_contract"],
            "readers": ["Service Account bot-query"],
            "notes": ["Alerta de schema drift habilitado."],
        },
        typing_notes=["cliente_id exige CAST para JOIN com STRING."],
        pending_technical=["[PENDENCIA TECNICA] Revisar descricao da coluna segmento_cliente."],
    )

    result = finalize_document_markdown(state)
    markdown = result["markdown_document"]

    assert "## 1. Visao geral (Overview)" in markdown
    assert "## Header" in markdown
    assert "## Diagrama de fluxo (Mermaid)" in markdown
    assert "## 2. Dicionario de dados (Data Dictionary)" in markdown
    assert "## 3. Checklist de qualidade de dados (DQ)" in markdown
    assert "## 4. Governanca (Dataplex/Catalog)" in markdown
    assert "[PENDENCIA TECNICA]" in markdown


def test_merge_governance_with_dataplex_appends_aspects_and_glossary():
    governance = {
        "aspect_types": ["schema_contract"],
        "readers": ["Service Account bot-query"],
        "notes": [],
    }
    dataplex_context = {
        "aspect_types": ["data_quality_profile"],
        "business_glossary": ["Receita Liquida"],
        "entry_name": "projects/p/locations/us/entryGroups/@bigquery/entries/t",
    }

    merged = _merge_governance_with_dataplex(governance, dataplex_context)

    assert "schema_contract" in merged["aspect_types"]
    assert "data_quality_profile" in merged["aspect_types"]
    assert any(note.startswith("Glossario:") for note in merged["notes"])


def test_extract_explicit_table_reference_full_name():
    text = "Gerar data contract da tabela silviosalviati.inteligencia_negocios.fatos_vendas"

    result = _extract_explicit_table_reference(text)

    assert result["project"] == "silviosalviati"
    assert result["dataset"] == "inteligencia_negocios"
    assert result["table"] == "fatos_vendas"


def test_extract_explicit_table_reference_ignores_free_text_tokens():
    text = "Quero um documento com ticket_medio por batch_diario"

    result = _extract_explicit_table_reference(text)

    assert result["table"] == ""


def test_parse_document_request_reads_structured_blocks():
    state = DocumentBuildState(
        request_text=(
            "[TABELA]\n"
            "silviosalviati.inteligencia_negocios.fatos_vendas\n\n"
            "[OBJETIVO]\n"
            "Documentar a tabela para consumo em BI.\n\n"
            "[CONTEXTO DE NEGÓCIO]\n"
            "Base para KPI de receita e margem.\n\n"
            "[TIPO DE DOC]\n"
            "documentacao_funcional\n"
        ),
        project_id="silviosalviati",
    )

    parsed = parse_document_request(state)

    assert parsed["table_name"] == "fatos_vendas"
    assert parsed["doc_type"] == "documentacao_funcional"
    assert parsed["objective"].startswith("Documentar a tabela")


def test_finalize_markdown_hides_project_and_dataset_hint():
    state = DocumentBuildState(
        request_text="Gerar doc",
        project_id="silviosalviati",
        dataset_hint="inteligencia_negocios",
        title="Doc",
        doc_type="documentacao_funcional",
        table_name="fatos_vendas",
        table_path="silviosalviati.inteligencia_negocios.fatos_vendas",
        summary="Resumo",
        objective="Objetivo",
        frequency="Batch diario",
    )

    result = finalize_document_markdown(state)
    markdown = result["markdown_document"]

    assert "Project ID" not in markdown
    assert "Dataset hint" not in markdown
