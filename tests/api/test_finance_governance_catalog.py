"""Teste do endpoint admin de reindexação do catálogo (RAG)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api.dependencies import get_admin_user
from src.api.routes import finance_governance as fg_module
from src.api.routes.finance_governance import router as finance_governance_router


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(finance_governance_router)
    app.dependency_overrides[get_admin_user] = lambda: {
        "token": "admin-token",
        "username": "admin",
        "is_admin": True,
    }
    return TestClient(app)


def test_catalog_reindex_chama_catalog_index_com_force():
    client = _build_client()

    with patch.object(
        fg_module.catalog_index,
        "reindex_catalog",
        return_value={"reindexed": True, "tables_indexed": 9, "datasets": 3},
    ) as mock_reindex:
        res = client.post(
            "/admin/finance/catalog/reindex",
            json={"project_id": "silviosalviati", "force": True},
        )

    assert res.status_code == 200
    body = res.json()
    assert body["reindexed"] is True
    assert body["tables_indexed"] == 9
    mock_reindex.assert_called_once_with("silviosalviati", force=True)


def test_catalog_reindex_default_force_false():
    client = _build_client()

    with patch.object(
        fg_module.catalog_index, "reindex_catalog", return_value={"reindexed": False}
    ) as mock_reindex:
        res = client.post(
            "/admin/finance/catalog/reindex", json={"project_id": "silviosalviati"}
        )

    assert res.status_code == 200
    mock_reindex.assert_called_once_with("silviosalviati", force=False)
