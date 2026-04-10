from src.agents.document_build import DocumentBuildAgent
from src.agents.document_build.nodes import finalize_document_markdown
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
