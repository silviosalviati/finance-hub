"""Agente SchemaGraphExplorer — Introspecção e grafo de relacionamentos BigQuery.

Dado um GCP Project ID e opcionalmente um ou mais datasets, o agente:
1. Introspecta via BQ API todos os datasets/tabelas do projeto
2. Analisa schemas de cada tabela
3. Infere relacionamentos usando estratégias EXPLÍCITA, SEMÂNTICA e TEMPORAL
4. Enriquece relacionamentos com LLM (categorização + descrição em PT)
5. Retorna payload de grafo para visualização no frontend

Arquitetura LangGraph (pipeline linear):
  discover_datasets → discover_tables → infer_relationships
      → enrich_with_llm → build_graph_payload
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.agents.schema_graph.graph import build_graph
from src.agents.schema_graph.state import SchemaGraphState
from src.core.base_agent import BaseAgent
from src.shared.config import VERTEXAI_MODEL
from src.shared.tools.llm import create_llm


class SchemaGraphAgent(BaseAgent):
    """Agente que introspecta BigQuery e constrói grafo de relacionamentos."""

    def __init__(self) -> None:
        self._graph = None

    @property
    def agent_id(self) -> str:
        return "schema_graph"

    @property
    def display_name(self) -> str:
        return "Schema Explorer"

    def _get_graph(self):
        if self._graph is None:
            self._graph = build_graph(create_llm())
        return self._graph

    def analyze(
        self,
        query: str,
        project_id: str,
        dataset_hint: str | None = None,
    ) -> dict[str, Any]:
        """Executa a introspecção e retorna o payload do grafo.

        Args:
            query: não utilizado diretamente; pode conter dataset_filter
                   no formato "dataset1,dataset2" ou ser ignorado.
            project_id: GCP Project ID obrigatório.
            dataset_hint: datasets a filtrar separados por vírgula.
                          String vazia ou None = todos os datasets.
        """
        graph = self._get_graph()

        # Extrai dataset_filter de dataset_hint (separados por vírgula)
        dataset_filter: list[str] = []
        if dataset_hint:
            dataset_filter = [
                d.strip() for d in dataset_hint.split(",") if d.strip()
            ]

        # max_tables pode vir na query como "max_tables=50"
        max_tables = 30
        if query:
            match_max = __import__("re").search(r"max_tables\s*=\s*(\d+)", query)
            if match_max:
                max_tables = min(int(match_max.group(1)), 100)

        initial_state = SchemaGraphState(
            project_id=project_id,
            dataset_filter=dataset_filter,
            max_tables_per_dataset=max_tables,
            datasets=[],
            tables=[],
            raw_relationships=[],
            relationships=[],
            graph_nodes=[],
            graph_edges=[],
            stats={},
            warnings=[],
            error=None,
        )

        final_state: dict[str, Any] = {}
        for event in graph.stream(initial_state, stream_mode="values"):
            final_state = event

        if not final_state:
            return {
                "status": "error",
                "error": "Pipeline não produziu resultado.",
            }

        error = final_state.get("error")
        if error:
            return {
                "status": "error",
                "error": error,
                "warnings": final_state.get("warnings", []),
            }

        return {
            "status": "ok",
            "project_id": project_id,
            "graph_nodes": final_state.get("graph_nodes", []),
            "graph_edges": final_state.get("graph_edges", []),
            "stats": final_state.get("stats", {}),
            "tables": final_state.get("tables", []),
            "datasets": final_state.get("datasets", []),
            "warnings": final_state.get("warnings", []),
            "analyzed_at": datetime.utcnow().isoformat(),
        }

    def runtime_info(self) -> dict[str, str]:
        return {
            "agent_id": self.agent_id,
            "display_name": self.display_name,
            "model": VERTEXAI_MODEL or "nao definido",
            "provider": "vertexai",
        }
