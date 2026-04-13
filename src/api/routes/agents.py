from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from src.api.dependencies import get_checkpointer, get_current_user, get_registry
from src.shared.tools.bigquery import (
    validate_dataset_for_query_build,
    validate_query_context_for_query_analyzer,
)

router = APIRouter(tags=["agents"])


class AnalyzeRequest(BaseModel):
    query: str
    project_id: str | None = None
    dataset_hint: str | None = None


class ValidateDatasetRequest(BaseModel):
    project_id: str
    dataset_hint: str


class ValidateAnalyzerContextRequest(BaseModel):
    query: str
    project_id: str | None = None


@router.get("/api/runtime-llm")
async def runtime_llm_info():
    registry = get_registry()
    agent = registry.get("query_analyzer")
    return agent.runtime_info()


@router.get("/api/agents")
async def list_agents():
    registry = get_registry()
    return {"agents": registry.list_ids()}


@router.post("/analyze")
async def analyze(
    req: AnalyzeRequest,
    session: dict[str, Any] = Depends(get_current_user),
):
    query = req.query.strip()
    project_id = req.project_id.strip() if req.project_id else ""

    if not query:
        raise HTTPException(status_code=400, detail="Query nao pode ser vazia.")

    try:
        registry = get_registry()
        agent = registry.get("query_analyzer")
        result = agent.analyze(query=query, project_id=project_id, dataset_hint=req.dataset_hint)
        checkpoint_key = f"{session['token']}-query_analyzer"
        get_checkpointer().save(checkpoint_key, result)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/agents/{agent_id}/analyze")
async def analyze_by_agent(
    agent_id: str,
    req: AnalyzeRequest,
    session: dict[str, Any] = Depends(get_current_user),
):
    query = req.query.strip()
    project_id = req.project_id.strip() if req.project_id else ""

    if not query:
        raise HTTPException(status_code=400, detail="Query nao pode ser vazia.")
    if agent_id != "query_analyzer" and not project_id:
        raise HTTPException(status_code=400, detail="Project ID nao pode ser vazio.")

    registry = get_registry()
    try:
        agent = registry.get(agent_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        result = agent.analyze(query=query, project_id=project_id, dataset_hint=req.dataset_hint)
        checkpoint_key = f"{session['token']}-{agent_id}"
        get_checkpointer().save(checkpoint_key, result)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/agents/query_build/validate-dataset")
async def validate_query_build_dataset(
    req: ValidateDatasetRequest,
    _session: dict[str, Any] = Depends(get_current_user),
):
    project_id = req.project_id.strip()
    dataset_hint = req.dataset_hint.strip()

    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID nao pode ser vazio.")
    if not dataset_hint:
        raise HTTPException(status_code=400, detail="Dataset hint nao pode ser vazio.")

    try:
        return validate_dataset_for_query_build(
            project_id=project_id,
            dataset_hint=dataset_hint,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/api/agents/query_analyzer/validate-query-context")
async def validate_query_analyzer_context(
    req: ValidateAnalyzerContextRequest,
    _session: dict[str, Any] = Depends(get_current_user),
):
    query = req.query.strip()
    project_id = req.project_id.strip() if req.project_id else None

    if not query:
        raise HTTPException(status_code=400, detail="Query nao pode ser vazia.")

    try:
        return validate_query_context_for_query_analyzer(
            query=query,
            project_id=project_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/api/agents/{agent_id}/checkpoint")
async def get_agent_checkpoint(
    agent_id: str,
    session: dict[str, Any] = Depends(get_current_user),
):
    checkpoint_key = f"{session['token']}-{agent_id}"
    payload = get_checkpointer().load(checkpoint_key)
    if payload is None:
        return {"status": "empty", "checkpoint": None}
    return {"status": "ok", "checkpoint": payload}
