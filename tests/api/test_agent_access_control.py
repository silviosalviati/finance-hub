"""Testes do controle de acesso a agentes por tag de categoria:

- `GET /api/agents` filtra por tag para usuários não-admin (admin vê tudo).
- Agente sem tag fica visível para todos (default seguro de rollout).
- `POST /api/agents/{agent_id}/analyze` bloqueia (403) usuário sem tag
  compatível — a filtragem do frontend é só UX, esta é a checagem real.
- Rotas admin de classificação (`GET/PUT /admin/agents*`).
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api.dependencies import get_admin_user, get_current_user
from src.api.routes import admin as admin_module
from src.api.routes import agents as agents_module
from src.api.routes.admin import router as admin_router
from src.api.routes.agents import router as agents_router


class _DummyAgent:
    def analyze(self, query: str, project_id: str, dataset_hint: str | None = None):
        return {"status": "ok", "query": query}


class _DummyCheckpointer:
    def __init__(self) -> None:
        self._data: dict[str, object] = {}

    def save(self, key, payload):
        self._data[key] = payload

    def load(self, key):
        return self._data.get(key)


class _DummyRegistry:
    def __init__(self, ids: list[str]) -> None:
        self._ids = ids
        self._agent = _DummyAgent()

    def get(self, agent_id: str):
        if agent_id not in self._ids:
            raise KeyError(agent_id)
        return self._agent

    def list_ids(self):
        return list(self._ids)


@pytest.fixture()
def db(tmp_path, monkeypatch):
    from src.core import database

    monkeypatch.setattr(database, "_DB_PATH", tmp_path / "test_app.db")
    database.init_db()
    return database


def _client_as(session: dict) -> TestClient:
    app = FastAPI()
    app.include_router(agents_router)
    app.include_router(admin_router)
    app.dependency_overrides[get_current_user] = lambda: session
    if session.get("is_admin"):
        app.dependency_overrides[get_admin_user] = lambda: session
    return TestClient(app)


REGISTRY = _DummyRegistry(["query_analyzer", "document_build", "finance_auditor"])


class TestListAgentsFiltering:
    def test_agente_sem_tag_e_visivel_para_usuario_sem_tags(self, db):
        client = _client_as({"token": "t", "username": "u1", "is_admin": False})
        db.create_user("u1", "senha123", "U1", False)

        with patch.object(agents_module, "get_registry", return_value=REGISTRY):
            res = client.get("/api/agents")
        assert res.status_code == 200
        ids = {a["agent_id"] for a in res.json()["agents"]}
        assert ids == {"query_analyzer", "document_build", "finance_auditor"}

    def test_usuario_so_ve_agente_com_tag_compativel(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops"])
        db.create_user("u2", "senha123", "U2", False, agent_tags="finops")
        client = _client_as({"token": "t", "username": "u2", "is_admin": False})

        with patch.object(agents_module, "get_registry", return_value=REGISTRY):
            res = client.get("/api/agents")
        ids = {a["agent_id"] for a in res.json()["agents"]}
        assert ids == {"query_analyzer", "document_build", "finance_auditor"}

    def test_usuario_sem_tag_compativel_nao_ve_agente_classificado(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops"])
        db.create_user("u3", "senha123", "U3", False, agent_tags="documentacao")
        client = _client_as({"token": "t", "username": "u3", "is_admin": False})

        with patch.object(agents_module, "get_registry", return_value=REGISTRY):
            res = client.get("/api/agents")
        ids = {a["agent_id"] for a in res.json()["agents"]}
        assert ids == {"query_analyzer", "document_build"}
        assert "finance_auditor" not in ids

    def test_admin_ve_todos_agentes_independente_de_tags(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops"])
        client = _client_as({"token": "t", "username": "admin", "is_admin": True})

        with patch.object(agents_module, "get_registry", return_value=REGISTRY):
            res = client.get("/api/agents")
        ids = {a["agent_id"] for a in res.json()["agents"]}
        assert ids == {"query_analyzer", "document_build", "finance_auditor"}


class TestAnalyzeEndpointEnforcement:
    def test_403_quando_usuario_sem_tag_chama_agente_restrito_direto(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops"])
        db.create_user("u4", "senha123", "U4", False, agent_tags="documentacao")
        client = _client_as({"token": "t", "username": "u4", "is_admin": False})

        with patch.object(agents_module, "get_registry", return_value=REGISTRY):
            res = client.post(
                "/api/agents/finance_auditor/analyze",
                json={"query": "SELECT 1", "project_id": "proj"},
            )
        assert res.status_code == 403

    def test_200_quando_usuario_tem_tag_compativel(self, db):
        # document_build (não finance_auditor) — este último tem um fluxo
        # conversacional bem mais complexo (chat session, RAG), fora do
        # escopo deste teste, que é só o gate de acesso por tag.
        db.upsert_agent_tags("document_build", ["documentacao"])
        db.create_user("u5", "senha123", "U5", False, agent_tags="documentacao")
        client = _client_as({"token": "t", "username": "u5", "is_admin": False})

        with patch.object(agents_module, "get_registry", return_value=REGISTRY), patch.object(
            agents_module, "get_checkpointer", return_value=_DummyCheckpointer()
        ):
            res = client.post(
                "/api/agents/document_build/analyze",
                json={"query": "SELECT 1", "project_id": "proj"},
            )
        assert res.status_code == 200


class TestAdminAgentClassificationRoutes:
    def test_get_admin_agents_lista_metadados_e_tags(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops"])
        client = _client_as({"token": "t", "username": "admin", "is_admin": True})

        with patch.object(admin_module, "get_registry", return_value=REGISTRY):
            res = client.get("/admin/agents")
        assert res.status_code == 200
        by_id = {a["agent_id"]: a for a in res.json()}
        assert by_id["finance_auditor"]["tags"] == ["finops"]
        assert by_id["query_analyzer"]["tags"] == []

    def test_put_agent_tags_atualiza_e_persiste(self, db):
        client = _client_as({"token": "t", "username": "admin", "is_admin": True})

        with patch.object(admin_module, "get_registry", return_value=REGISTRY):
            res = client.put(
                "/admin/agents/document_build/tags", json={"tags": ["documentacao"]}
            )
        assert res.status_code == 200
        assert db.get_agent_tags("document_build") == ["documentacao"]

    def test_put_agent_tags_404_para_agente_inexistente(self, db):
        client = _client_as({"token": "t", "username": "admin", "is_admin": True})

        with patch.object(admin_module, "get_registry", return_value=REGISTRY):
            res = client.put("/admin/agents/nao_existe/tags", json={"tags": ["x"]})
        assert res.status_code == 404

    def test_get_agent_tags_distintas(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops"])
        db.create_user("u6", "senha123", "U6", False, agent_tags="documentacao")
        client = _client_as({"token": "t", "username": "admin", "is_admin": True})

        res = client.get("/admin/agent-tags")
        assert res.status_code == 200
        assert set(res.json()) == {"finops", "documentacao"}

    def test_delete_agent_tag_remove_de_agentes_e_usuarios(self, db):
        db.upsert_agent_tags("finance_auditor", ["finops"])
        db.create_user("u7", "senha123", "U7", False, agent_tags="finops")
        client = _client_as({"token": "t", "username": "admin", "is_admin": True})

        res = client.delete("/admin/agent-tags/finops")
        assert res.status_code == 200
        assert res.json() == {"agents_updated": 1, "users_updated": 1}
        assert db.get_agent_tags("finance_auditor") == []
        assert db.get_user("u7")["agent_tags"] == ""
