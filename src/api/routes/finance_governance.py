"""Endpoints de governança do Finance Voice IA (Fase 3).

Semantic Layer (métricas), RBAC por usuário e leitura do audit trail.
Todos os endpoints requerem usuário admin.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from src.api.dependencies import get_admin_user
from src.core.database import (
    append_finance_audit,
    delete_finance_metric,
    get_finance_acl,
    get_finance_metric,
    list_finance_acl,
    list_finance_audit,
    list_finance_metrics,
    upsert_finance_acl,
    upsert_finance_metric,
)

router = APIRouter(prefix="/admin/finance", tags=["finance-governance"])


# ── Semantic Layer ──────────────────────────────────────────────────────────

class MetricUpsertRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)
    source_table: str = Field(default="", max_length=200)
    sql_template: str = Field(..., min_length=1, max_length=8000)
    owner: str = Field(default="", max_length=120)
    tags: str = Field(default="", max_length=300)


@router.get("/metrics")
async def list_metrics(_admin: dict[str, Any] = Depends(get_admin_user)):
    return {"metrics": list_finance_metrics()}


@router.get("/metrics/{key}")
async def get_metric(key: str, _admin: dict[str, Any] = Depends(get_admin_user)):
    m = get_finance_metric(key)
    if not m:
        raise HTTPException(status_code=404, detail="Métrica não encontrada.")
    return m


@router.put("/metrics/{key}")
async def upsert_metric(
    key: str,
    req: MetricUpsertRequest,
    _admin: dict[str, Any] = Depends(get_admin_user),
):
    result = upsert_finance_metric(
        key=key,
        name=req.name,
        description=req.description,
        source_table=req.source_table,
        sql_template=req.sql_template,
        owner=req.owner,
        tags=req.tags,
    )
    return result


@router.delete("/metrics/{key}", status_code=204)
async def remove_metric(key: str, _admin: dict[str, Any] = Depends(get_admin_user)):
    ok = delete_finance_metric(key)
    if not ok:
        raise HTTPException(status_code=404, detail="Métrica não encontrada.")
    return None


# ── RBAC ────────────────────────────────────────────────────────────────────

class AclUpsertRequest(BaseModel):
    allowed_datasets: list[str] = Field(default_factory=list)
    allowed_metrics: list[str] = Field(default_factory=list)
    denied_datasets: list[str] = Field(default_factory=list)


@router.get("/acl")
async def list_acl(_admin: dict[str, Any] = Depends(get_admin_user)):
    return {"acl": list_finance_acl()}


@router.get("/acl/{user_id}")
async def get_acl(user_id: str, _admin: dict[str, Any] = Depends(get_admin_user)):
    acl = get_finance_acl(user_id)
    if not acl:
        raise HTTPException(status_code=404, detail="ACL não encontrada.")
    return acl


@router.put("/acl/{user_id}")
async def upsert_acl(
    user_id: str,
    req: AclUpsertRequest,
    _admin: dict[str, Any] = Depends(get_admin_user),
):
    return upsert_finance_acl(
        user_id=user_id,
        allowed_datasets=req.allowed_datasets,
        allowed_metrics=req.allowed_metrics,
        denied_datasets=req.denied_datasets,
    )


# ── Audit trail ─────────────────────────────────────────────────────────────

@router.get("/audit")
async def get_audit(
    limit: int = 50,
    user_id: str | None = None,
    _admin: dict[str, Any] = Depends(get_admin_user),
):
    return {"entries": list_finance_audit(limit=limit, user_id=user_id)}


# ── Re-export para conveniência em testes / observabilidade ─────────────────

@router.post("/audit/_test", include_in_schema=False)
async def _audit_test_append(
    entry: dict[str, Any],
    _admin: dict[str, Any] = Depends(get_admin_user),
):
    audit_id = append_finance_audit(entry)
    return {"id": audit_id}
