"""Testes do endpoint POST /api/agents/finance_auditor/gerencia e da
propagacao do dataset fixado (`pinned_dataset_ref`) para `/analyze`.

`get_registry()` e `get_checkpointer()` sao chamados como funcoes globais
dentro das rotas (nao via `Depends`), entao para isola-los nos testes
precisamos fazer `patch.object` direto no modulo de rotas — sobrescrever via
`app.dependency_overrides` nao tem efeito sobre eles (apenas sobre
`get_current_user`, que de fato e' injetado via `Depends`).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api.dependencies import get_current_user
from src.api.routes import agents as agents_module
from src.api.routes.agents import router as agents_router


class _DummyCheckpointer:
    def __init__(self) -> None:
        self._data: dict[str, object] = {}

    def save(self, key, payload):
        self._data[key] = payload

    def load(self, key):
        return self._data.get(key)


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(agents_router)
    app.dependency_overrides[get_current_user] = lambda: {
        "token": "test-token",
        "username": "tester",
        "is_admin": False,
    }
    return TestClient(app)


async def _fake_suggestions(_tables):
    return ["Pergunta 1", "Pergunta 2"]


def _sse_events(raw: str) -> list[dict]:
    """Decodifica o corpo `text/event-stream` em uma lista de payloads JSON."""
    events = []
    for block in raw.split("\n\n"):
        for line in block.strip().splitlines():
            if line.startswith("data:"):
                events.append(json.loads(line[len("data:"):].strip()))
    return events


def _final_sse_event(raw: str) -> dict:
    """Último evento do stream com `status` — o resultado final do endpoint."""
    for event in reversed(_sse_events(raw)):
        if "status" in event:
            return event
    raise AssertionError(f"Nenhum evento SSE com 'status' encontrado: {raw!r}")


def test_gerencia_not_found_quando_sem_match():
    client = _build_client()

    with patch.object(agents_module, "resolve_dataset_by_gerencia", return_value=None):
        res = client.post("/api/agents/finance_auditor/gerencia", json={"gerencia": "cobranca"})

    assert res.status_code == 200
    assert _final_sse_event(res.text)["status"] == "not_found"


def test_gerencia_denied_quando_rbac_nega():
    client = _build_client()

    with patch.object(
        agents_module,
        "resolve_dataset_by_gerencia",
        return_value={"dataset_id": "ecommerce_saude", "gerencia": "experiencia_cliente", "label_key": "gerencia"},
    ), patch.object(agents_module.finance_rbac, "check_dataset", return_value=(False, "negado")):
        res = client.post(
            "/api/agents/finance_auditor/gerencia", json={"gerencia": "experiencia_cliente"}
        )

    assert res.status_code == 200
    assert _final_sse_event(res.text)["status"] == "denied"


def test_gerencia_ok_fixa_dataset_na_sessao_e_retorna_sugestoes():
    client = _build_client()
    checkpointer = _DummyCheckpointer()

    fake_catalog = {
        "dataset_ref": "silviosalviati.ecommerce_saude",
        "tables": [
            {
                "table_id": "clientes",
                "full_name": "silviosalviati.ecommerce_saude.clientes",
                "columns": [
                    {"name": "id_cliente", "type": "STRING", "mode": "NULLABLE", "description": "PK"}
                ],
            }
        ],
    }

    with patch.object(
        agents_module,
        "resolve_dataset_by_gerencia",
        return_value={"dataset_id": "ecommerce_saude", "gerencia": "experiencia_cliente", "label_key": "gerencia"},
    ), patch.object(agents_module.finance_rbac, "check_dataset", return_value=(True, "")), \
         patch.object(agents_module, "_get_cached_dataset_catalog", return_value=fake_catalog), \
         patch.object(agents_module, "_generate_gerencia_suggestions", side_effect=_fake_suggestions), \
         patch.object(agents_module, "get_checkpointer", return_value=checkpointer):
        res = client.post(
            "/api/agents/finance_auditor/gerencia",
            json={"gerencia": "experiencia_cliente", "project_id": "silviosalviati"},
        )

    assert res.status_code == 200
    body = _final_sse_event(res.text)
    assert body["status"] == "ok"
    assert body["dataset_ref"] == "silviosalviati.ecommerce_saude"
    assert body["table_count"] == 1
    assert body["suggestions"] == ["Pergunta 1", "Pergunta 2"]

    saved = checkpointer.load("test-token-finance_auditor-chat")
    assert saved["profile"]["pinned_dataset_ref"] == "silviosalviati.ecommerce_saude"
    assert saved["profile"]["pinned_gerencia"] == "experiencia_cliente"


def test_analyze_usa_pinned_dataset_ref_quando_request_nao_informa():
    client = _build_client()
    checkpointer = _DummyCheckpointer()
    checkpointer.save(
        "test-token-finance_auditor-chat",
        {"turns": [], "profile": {"pinned_dataset_ref": "silviosalviati.ecommerce_saude"}},
    )

    captured: dict[str, object] = {}

    class _FakeAgent:
        def analyze(self, **kwargs):
            captured.update(kwargs)
            return {"status": "ok", "markdown_report": "ok", "chat_answer": "ok"}

    class _FakeRegistry:
        def get(self, _agent_id):
            return _FakeAgent()

    with patch.object(agents_module, "get_registry", return_value=_FakeRegistry()), \
         patch.object(agents_module, "get_checkpointer", return_value=checkpointer):
        res = client.post(
            "/api/agents/finance_auditor/analyze",
            json={"query": "quanto vendemos no total?", "project_id": "silviosalviati"},
        )

    assert res.status_code == 200
    assert captured.get("dataset_hint") == "silviosalviati.ecommerce_saude"


def test_analyze_request_dataset_hint_tem_prioridade_sobre_pin():
    client = _build_client()
    checkpointer = _DummyCheckpointer()
    checkpointer.save(
        "test-token-finance_auditor-chat",
        {"turns": [], "profile": {"pinned_dataset_ref": "silviosalviati.ecommerce_saude"}},
    )

    captured: dict[str, object] = {}

    class _FakeAgent:
        def analyze(self, **kwargs):
            captured.update(kwargs)
            return {"status": "ok", "markdown_report": "ok", "chat_answer": "ok"}

    class _FakeRegistry:
        def get(self, _agent_id):
            return _FakeAgent()

    with patch.object(agents_module, "get_registry", return_value=_FakeRegistry()), \
         patch.object(agents_module, "get_checkpointer", return_value=checkpointer):
        res = client.post(
            "/api/agents/finance_auditor/analyze",
            json={
                "query": "quanto vendemos no total?",
                "project_id": "silviosalviati",
                "dataset_hint": "silviosalviati.outro_dataset",
            },
        )

    assert res.status_code == 200
    assert captured.get("dataset_hint") == "silviosalviati.outro_dataset"
