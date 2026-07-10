"""RBAC do Finance Voice IA — verificação por dataset e por métrica.

A ACL fica em SQLite (`finance_user_acl`). Por padrão (``FINANCE_AUDITOR_RBAC_STRICT``
"1"/"true"), um usuário sem ACL configurada — ou com ACL sem allowlist — não
tem acesso a nenhum dataset/métrica. Para conceder acesso a todas as áreas
(ex.: diretor), configure a ACL do usuário com allowlist ``"*"`` em vez de
desligar o modo strict globalmente. Só desligue o modo strict (runtime config
"0"/"false") se quiser voltar ao modo aberto antigo, onde a ausência de ACL
libera acesso.

Admins (flag `is_admin` na sessão) sempre passam.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from src.core.database import get_finance_acl
from src.shared.config import get_runtime_config


def _strict_mode() -> bool:
    val = (get_runtime_config("FINANCE_AUDITOR_RBAC_STRICT", "1") or "1").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _slug(text: str) -> str:
    s = unicodedata.normalize("NFD", text or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9_]+", "_", s.lower()).strip("_")


def _match(target: str, allowed: list[str]) -> bool:
    target_slug = _slug(target)
    for raw in allowed:
        raw = (raw or "").strip()
        if not raw:
            continue
        if raw == "*":
            return True
        # Wildcard sufixo: "logistica_*" casa com qualquer começando por "logistica"
        if raw.endswith("*"):
            prefix = _slug(raw[:-1])
            if prefix and target_slug.startswith(prefix):
                return True
            continue
        if _slug(raw) == target_slug:
            return True
    return False


def _resolve_acl(user: dict[str, Any] | None) -> dict[str, Any] | None:
    if not user:
        return None
    uid = str(user.get("username") or user.get("user_id") or "").strip()
    if not uid:
        return None
    try:
        return get_finance_acl(uid)
    except Exception:  # noqa: BLE001
        return None


def check_dataset(user: dict[str, Any] | None, dataset: str) -> tuple[bool, str]:
    """Retorna (permitido, motivo)."""
    if not dataset:
        return True, ""
    if user and user.get("is_admin"):
        return True, ""
    acl = _resolve_acl(user)
    if acl is None:
        if _strict_mode():
            return False, f"Sem ACL configurada para o usuário (modo strict)."
        return True, ""
    denied = acl.get("denied_datasets") or []
    if denied and _match(dataset, denied):
        return False, f"Dataset '{dataset}' explicitamente negado para o usuário."
    allowed = acl.get("allowed_datasets") or []
    if not allowed:
        # ACL existe mas sem regras de allow → strict bloqueia, senão libera
        if _strict_mode():
            return False, f"Sem allowlist de datasets para o usuário."
        return True, ""
    if _match(dataset, allowed):
        return True, ""
    return False, f"Dataset '{dataset}' não está na allowlist do usuário."


def check_metric(user: dict[str, Any] | None, metric_key: str) -> tuple[bool, str]:
    if not metric_key:
        return True, ""
    if user and user.get("is_admin"):
        return True, ""
    acl = _resolve_acl(user)
    if acl is None:
        return (not _strict_mode()), (
            "Sem ACL configurada (modo strict)." if _strict_mode() else ""
        )
    allowed = acl.get("allowed_metrics") or []
    if not allowed:
        return (not _strict_mode()), (
            "Sem allowlist de métricas para o usuário." if _strict_mode() else ""
        )
    if _match(metric_key, allowed):
        return True, ""
    return False, f"Métrica '{metric_key}' não está na allowlist do usuário."


def project_from_table_ref(table_ref: str) -> tuple[str, str]:
    """Extrai (project, dataset) de 'projeto.dataset.tabela' (best effort)."""
    parts = (table_ref or "").split(".")
    if len(parts) >= 3:
        return parts[0], parts[1]
    if len(parts) == 2:
        return "", parts[0]
    return "", ""


__all__ = [
    "check_dataset",
    "check_metric",
    "project_from_table_ref",
]
