"""Endpoints de governança do Finance Voice IA (Fase 3).

Semantic Layer (métricas), RBAC por usuário e leitura do audit trail.
Todos os endpoints requerem usuário admin.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from src.api.dependencies import get_admin_user
from src.agents.finance_auditor import alerting, catalog_index, org_memory
from src.core.database import (
    append_finance_audit,
    delete_finance_metric,
    delete_org_fact,
    get_finance_acl,
    get_finance_metric,
    list_finance_acl,
    list_finance_audit,
    list_finance_metrics,
    list_org_facts,
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
    # Fase 4: threshold opcional (JSON string).
    alert_threshold: str = Field(default="", max_length=1000)
    # Gold Metric Catalog: domínio de negócio (ex.: "cobranca", "vendas") e
    # se é a métrica oficial desse domínio — usado pelo Planner para montar
    # gráfico/dashboard automaticamente quando o usuário não cita uma métrica.
    domain: str = Field(default="", max_length=120)
    is_official: bool = False


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
        alert_threshold=req.alert_threshold,
        domain=req.domain,
        is_official=req.is_official,
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


# ── Fase 4: memória organizacional (admin) ──────────────────────────────────

class OrgFactCreateRequest(BaseModel):
    user_id: str = Field(default="", max_length=120)
    fact_text: str = Field(..., min_length=1, max_length=1000)
    tags: str = Field(default="", max_length=200)
    scope: str = Field(default="user", pattern="^(user|global)$")


@router.get("/org-facts")
async def org_facts_list(
    user_id: str | None = None,
    include_global: bool = True,
    limit: int = 100,
    _admin: dict[str, Any] = Depends(get_admin_user),
):
    return {
        "facts": list_org_facts(
            user_id=user_id, include_global=include_global, limit=limit
        )
    }


@router.post("/org-facts", status_code=201)
async def org_facts_create(
    req: OrgFactCreateRequest,
    _admin: dict[str, Any] = Depends(get_admin_user),
):
    fact_id = org_memory.save_fact(
        user_id=req.user_id,
        fact_text=req.fact_text,
        tags=req.tags,
        scope=req.scope,
    )
    if not fact_id:
        raise HTTPException(status_code=400, detail="Falha ao salvar fato.")
    return {"id": fact_id}


@router.delete("/org-facts/{fact_id}", status_code=204)
async def org_facts_delete(
    fact_id: int,
    _admin: dict[str, Any] = Depends(get_admin_user),
):
    if not delete_org_fact(fact_id):
        raise HTTPException(status_code=404, detail="Fato não encontrado.")
    return None


# ── Fase 4: alerting (trigger externo / cron) ───────────────────────────────

class AlertsRunRequest(BaseModel):
    project_id: str = Field(..., min_length=1, max_length=200)
    params: dict[str, Any] | None = None


@router.post("/alerts/run")
async def alerts_run(
    req: AlertsRunRequest,
    admin: dict[str, Any] = Depends(get_admin_user),
):
    results = alerting.run_alerts(
        project_id=req.project_id,
        user=admin,
        params=req.params or {},
    )
    triggered = [r for r in results if r.get("triggered")]
    return {"evaluated": len(results), "triggered": len(triggered), "results": results}


# ── RAG do catálogo (datasets/tabelas/colunas por significado) ─────────────

class CatalogReindexRequest(BaseModel):
    project_id: str = Field(..., min_length=1, max_length=200)
    force: bool = False


@router.post("/catalog/reindex")
async def catalog_reindex(
    req: CatalogReindexRequest,
    _admin: dict[str, Any] = Depends(get_admin_user),
):
    """Reindexa o catálogo (embeddings de datasets/tabelas/colunas) usado pelo \
    Planner para achar dados por significado em vez de chutar nome de dataset.

    Sem `force`, respeita o TTL (`FINANCE_AUDITOR_CATALOG_TTL_HOURS`) — útil \
    para forçar atualização imediata depois de criar/alterar um dataset.

    Também sincroniza o Gold Metric Catalog (`GOLD_METRIC_CATALOG`, quando \
    existir em algum dataset do projeto) para o Semantic Layer — sem isso, \
    métricas oficiais recém-cadastradas no BigQuery só apareceriam depois do \
    próximo ciclo do warmup em background.
    """
    catalog_result = catalog_index.reindex_catalog(req.project_id, force=req.force)
    metrics_result = catalog_index.sync_gold_metric_catalog(req.project_id)
    return {"catalog": catalog_result, "gold_metric_catalog": metrics_result}
