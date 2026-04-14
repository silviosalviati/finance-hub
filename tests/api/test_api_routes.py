from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api.dependencies import get_checkpointer, get_current_user, get_registry
from src.api.routes.agents import router as agents_router
from src.api.routes.auth import router as auth_router


class _DummyAgent:
    def analyze(self, query: str, project_id: str, dataset_hint: str | None = None):
        return {
            "status": "ok",
            "query": query,
            "project_id": project_id,
            "dataset_hint": dataset_hint,
        }


class _DummyRegistry:
    def __init__(self) -> None:
        self._agent = _DummyAgent()

    def get(self, _agent_id: str):
        return self._agent

    def list_ids(self):
        return ["document_build", "query_analyzer", "query_build", "finance_auditor"]


class _DummyCheckpointer:
    def __init__(self) -> None:
        self._data = {}

    def save(self, key, payload):
        self._data[key] = payload

    def load(self, key):
        return self._data.get(key)


def _build_test_client_with_overrides() -> TestClient:
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(agents_router)

    checkpoint = _DummyCheckpointer()

    def _fake_current_user():
        return {
            "token": "test-token",
            "username": "tester",
            "name": "Test User",
            "login_at": "2026-01-01T00:00:00",
        }

    app.dependency_overrides[get_current_user] = _fake_current_user
    app.dependency_overrides[get_registry] = lambda: _DummyRegistry()
    app.dependency_overrides[get_checkpointer] = lambda: checkpoint

    return TestClient(app)


def test_login_success_with_plain_password(monkeypatch):
    app = FastAPI()
    app.include_router(auth_router)

    def _fake_users():
        return {"tester": {"password": "123456", "name": "Test User"}}

    monkeypatch.setattr("src.api.routes.auth.load_users", _fake_users)

    client = TestClient(app)
    response = client.post("/api/login", json={"username": "tester", "password": "123456"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["username"] == "tester"
    assert payload["name"] == "Test User"
    assert isinstance(payload["token"], str)


def test_me_requires_authentication_without_bearer_header():
    app = FastAPI()
    app.include_router(auth_router)

    client = TestClient(app)
    response = client.get("/api/me")

    assert response.status_code == 401


def test_query_analyzer_analyze_and_checkpoint_flow():
    client = _build_test_client_with_overrides()

    analyze_response = client.post(
        "/api/agents/query_analyzer/analyze",
        json={
            "query": "SELECT 1",
            "project_id": None,
            "dataset_hint": None,
        },
    )
    assert analyze_response.status_code == 200

    checkpoint_response = client.get("/api/agents/query_analyzer/checkpoint")
    assert checkpoint_response.status_code == 200

    body = checkpoint_response.json()
    assert body["status"] == "ok"
    assert isinstance(body["checkpoint"], dict)
    assert "efficiency_score" in body["checkpoint"]
