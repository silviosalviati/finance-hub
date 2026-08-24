from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from src.api.dependencies import get_admin_user, get_registry
from src.core.agent_catalog import get_agent_meta
from src.core.database import (
    create_user,
    delete_tag_everywhere,
    delete_user,
    get_config_all,
    get_user,
    join_csv,
    list_agent_tag_assignments,
    list_distinct_tags,
    list_users,
    set_config_value,
    update_user,
    upsert_agent_tags,
)
from src.shared.config import get_default_gcp_project, invalidate_config_cache

router = APIRouter(prefix="/admin", tags=["admin"])


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=4, max_length=256)
    is_admin: bool = False
    gerencia: str = Field(default="", max_length=200)
    agent_tags: list[str] = Field(default_factory=list)


class UpdateUserRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=128)
    password: str | None = Field(default=None, min_length=4, max_length=256)
    is_admin: bool | None = None
    gerencia: str | None = Field(default=None, max_length=200)
    agent_tags: list[str] | None = None


class AgentTagsUpsertRequest(BaseModel):
    tags: list[str] = Field(default_factory=list)


class UpdateConfigRequest(BaseModel):
    value: str = Field(min_length=0, max_length=2048)


@router.get("/users")
async def admin_list_users(
    _admin: dict[str, Any] = Depends(get_admin_user),
) -> list[dict[str, Any]]:
    return list_users()


@router.get("/gerencias")
async def admin_list_gerencias(
    _admin: dict[str, Any] = Depends(get_admin_user),
) -> list[str]:
    from src.agents.finance_auditor.capabilities import list_all_gerencias

    project_id = get_default_gcp_project()
    return list_all_gerencias(project_id)


@router.post("/users", status_code=201)
async def admin_create_user(
    req: CreateUserRequest,
    _admin: dict[str, Any] = Depends(get_admin_user),
) -> dict[str, Any]:
    if get_user(req.username):
        raise HTTPException(status_code=409, detail="Usuário já existe.")
    return create_user(
        username=req.username,
        password=req.password,
        name=req.name,
        is_admin=req.is_admin,
        gerencia=req.gerencia,
        agent_tags=join_csv(req.agent_tags),
    )


@router.put("/users/{username}")
async def admin_update_user(
    username: str,
    req: UpdateUserRequest,
    admin: dict[str, Any] = Depends(get_admin_user),
) -> dict[str, Any]:
    if not get_user(username):
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")
    if username == admin["username"] and req.is_admin is False:
        raise HTTPException(
            status_code=400,
            detail="Não é possível remover seu próprio acesso de administrador.",
        )
    updated = update_user(
        username,
        name=req.name,
        password=req.password,
        is_admin=req.is_admin,
        gerencia=req.gerencia,
        agent_tags=join_csv(req.agent_tags) if req.agent_tags is not None else None,
    )
    if not updated:
        raise HTTPException(status_code=500, detail="Falha ao atualizar usuário.")
    return {"ok": True}


@router.delete("/users/{username}")
async def admin_delete_user(
    username: str,
    admin: dict[str, Any] = Depends(get_admin_user),
) -> dict[str, Any]:
    if username == admin["username"]:
        raise HTTPException(status_code=400, detail="Não é possível excluir o próprio usuário.")
    if not get_user(username):
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")
    deleted = delete_user(username)
    if not deleted:
        raise HTTPException(status_code=500, detail="Falha ao excluir usuário.")
    return {"ok": True}


@router.get("/config")
async def admin_get_config(
    _admin: dict[str, Any] = Depends(get_admin_user),
) -> list[dict[str, Any]]:
    return get_config_all()


@router.put("/config/{key}")
async def admin_update_config(
    key: str,
    req: UpdateConfigRequest,
    admin: dict[str, Any] = Depends(get_admin_user),
) -> dict[str, Any]:
    updated = set_config_value(key, req.value, updated_by=admin["username"])
    if not updated:
        raise HTTPException(status_code=404, detail="Parâmetro não encontrado.")
    invalidate_config_cache(key)
    return {"ok": True}


@router.get("/agents")
async def admin_list_agents(
    _admin: dict[str, Any] = Depends(get_admin_user),
) -> list[dict[str, Any]]:
    registry = get_registry()
    assignments = list_agent_tag_assignments()
    return [
        {
            "agent_id": agent_id,
            "display_name": get_agent_meta(agent_id).get("display_name", agent_id),
            "icon_token": get_agent_meta(agent_id).get("icon_token", ""),
            "color_token": get_agent_meta(agent_id).get("color_token", ""),
            "tags": assignments.get(agent_id, []),
        }
        for agent_id in registry.list_ids()
    ]


@router.put("/agents/{agent_id}/tags")
async def admin_update_agent_tags(
    agent_id: str,
    req: AgentTagsUpsertRequest,
    _admin: dict[str, Any] = Depends(get_admin_user),
) -> dict[str, Any]:
    registry = get_registry()
    try:
        registry.get(agent_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return upsert_agent_tags(agent_id, req.tags)


@router.get("/agent-tags")
async def admin_list_distinct_agent_tags(
    _admin: dict[str, Any] = Depends(get_admin_user),
) -> list[str]:
    return list_distinct_tags()


@router.delete("/agent-tags/{tag}")
async def admin_delete_agent_tag(
    tag: str,
    _admin: dict[str, Any] = Depends(get_admin_user),
) -> dict[str, Any]:
    return delete_tag_everywhere(tag)
