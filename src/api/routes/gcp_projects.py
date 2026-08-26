"""Endpoints de projetos/datasets GCP — compartilhados pelo seletor do Query Builder.

Extraído do antigo módulo do Schema Explorer (removido); mantido em módulo
próprio porque `_loadProjectsIntoSelect`/`_loadDatasetsIntoSelect`
(`static/js/scripts.js`) são usados tanto pelo picker de gerência do Query
Builder (`qb-project`/`qb-dataset`) quanto por qualquer futuro seletor de
projeto/dataset — não são exclusivos de um agente específico.
"""
from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from google.cloud import bigquery
from google.oauth2 import service_account

from src.api.dependencies import get_current_user
from src.shared.config import get_default_gcp_project, get_gcp_project_ids, get_runtime_config

router = APIRouter(tags=["gcp-projects"])

_CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform.read-only"


def _get_bq_client(project_id: str) -> bigquery.Client:
    creds_path = get_runtime_config("GOOGLE_APPLICATION_CREDENTIALS", "secrets/credentials.json")
    creds = service_account.Credentials.from_service_account_file(creds_path)
    return bigquery.Client(project=project_id, credentials=creds)


def _fetch_cloud_resource_manager_projects(creds_path: str) -> list[str]:
    from google.auth.transport.requests import Request as GRequest
    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=[_CLOUD_PLATFORM_SCOPE]
    )
    creds.refresh(GRequest())
    req = urllib.request.Request(
        "https://cloudresourcemanager.googleapis.com/v1/projects",
        headers={"Authorization": f"Bearer {creds.token}"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode())
    return sorted(
        p["projectId"]
        for p in data.get("projects", [])
        if p.get("lifecycleState") == "ACTIVE"
    )


@router.get("/api/schema-explorer/projects")
async def list_projects(
    session: dict[str, Any] = Depends(get_current_user),
) -> list[str]:
    """Return GCP project IDs — tries Cloud Resource Manager first, falls back to configured list."""
    configured = get_gcp_project_ids()
    creds_path = get_runtime_config("GOOGLE_APPLICATION_CREDENTIALS", "secrets/credentials.json")
    try:
        # Refresh de credencial + chamada HTTP — síncrono, roda numa thread.
        api_projects = await asyncio.to_thread(_fetch_cloud_resource_manager_projects, creds_path)
        if api_projects:
            # Merge: configured projects first (preserves preferred order), then any extras from API
            seen: set[str] = set()
            merged: list[str] = []
            for p in configured + [x for x in api_projects if x not in set(configured)]:
                if p not in seen:
                    seen.add(p)
                    merged.append(p)
            return merged
    except Exception:
        pass
    return configured


def _list_dataset_ids(project_id: str) -> list[str]:
    client = _get_bq_client(project_id)
    return sorted(ds.dataset_id for ds in client.list_datasets())


@router.get("/api/schema-explorer/datasets")
async def list_datasets(
    project_id: str = Query(default=""),
    session: dict[str, Any] = Depends(get_current_user),
) -> list[str]:
    """Return dataset IDs accessible in the given GCP project."""
    resolved = (project_id or get_default_gcp_project()).strip()
    if not resolved:
        raise HTTPException(status_code=400, detail="project_id é obrigatório.")
    try:
        # Chamada BigQuery — síncrona, roda numa thread.
        return await asyncio.to_thread(_list_dataset_ids, resolved)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao listar datasets: {exc}",
        ) from exc
