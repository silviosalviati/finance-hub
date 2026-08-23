"""Metadado de UI dos agentes (nome, descrição, ícone, view, badges cosméticas).

Mantido separado de `BaseAgent`/`AgentRegistry` (`src/core/base_agent.py`,
`src/core/registry.py`) porque estes cuidam de comportamento/execução do
agente, não de apresentação. `default_tags` aqui são badges puramente
cosméticas (mostradas no card) — não confundir com as tags de controle de
acesso administradas em `agent_tag_assignments` (`src/core/database.py`),
que a API expõe como `category_tags`.

Ícones não são duplicados aqui como SVG: cada agente só carrega um
`icon_token`/`color_token` que o frontend resolve num mapa de ícones já
existente em `static/js/scripts.js`.
"""
from __future__ import annotations

AGENT_CATALOG: dict[str, dict[str, str]] = {
    "query_analyzer": {
        "display_name": "SQL Review",
        "description": (
            "Revise custo e performance da SQL em segundos, com otimização "
            "pronta para produção."
        ),
        "view": "qa",
        "icon_token": "search",
        "color_token": "porto",
        "default_tags": "BigQuery,SQL,Performance",
    },
    "document_build": {
        "display_name": "Document Builder",
        "description": (
            "Converta schema e regras da tabela em documentação de alto "
            "impacto para negócio e engenharia: Markdown, HTML e Confluence."
        ),
        "view": "db",
        "icon_token": "file",
        "color_token": "teal",
        "default_tags": "Docs,Pipeline,DataOps",
    },
    "query_build": {
        "display_name": "Query Builder",
        "description": (
            "Transforme perguntas de negócio em SQL BigQuery confiável e "
            "acelere decisões com dados."
        ),
        "view": "qb",
        "icon_token": "branch",
        "color_token": "violet",
        "default_tags": "NL2SQL,BigQuery,IA",
    },
    "schema_graph": {
        "display_name": "Schema Explorer",
        "description": (
            "Visualize o diagrama ER de qualquer dataset BigQuery com "
            "relacionamentos inferidos e layout interativo."
        ),
        "view": "er",
        "icon_token": "diagram",
        "color_token": "violet",
        "default_tags": "Schema Explorer,BigQuery,DataOps",
    },
    "finance_auditor": {
        "display_name": "Finance Voice IA",
        "description": (
            "Converse com os dados da Diretoria Financeira — contas a "
            "pagar, receber, cobrança e experiência do cliente."
        ),
        "view": "audit",
        "icon_token": "shield",
        "color_token": "porto",
        "default_tags": "Financeiro,Cobrança,Contas,IA",
    },
}


def get_agent_meta(agent_id: str) -> dict[str, str]:
    return AGENT_CATALOG.get(agent_id, {})
