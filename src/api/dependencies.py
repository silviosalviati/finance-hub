from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import bcrypt
from fastapi import Depends, Header, HTTPException

from src.agents.document_build import DocumentBuildAgent
from src.agents.finance_auditor import FinanceAuditorAgent
from src.agents.query_analyzer import QueryAnalyzerAgent
from src.agents.query_build import QueryBuildAgent
from src.agents.schema_graph import SchemaGraphAgent
from src.core.checkpointer import CheckpointConfig, FileCheckpointer
from src.core.database import (
    count_sessions,
    create_session_row,
    delete_expired_sessions,
    delete_session_row,
    get_config_value,
    get_session_row,
    get_user,
)
from src.core.registry import AgentRegistry
from src.shared.config import get_runtime_config

_registry: AgentRegistry | None = None
_checkpointer: FileCheckpointer | None = None


def get_registry() -> AgentRegistry:
    global _registry
    if _registry is None:
        registry = AgentRegistry()
        registry.register(QueryAnalyzerAgent())
        registry.register(QueryBuildAgent())
        registry.register(DocumentBuildAgent())
        registry.register(FinanceAuditorAgent())
        registry.register(SchemaGraphAgent())
        _registry = registry
    return _registry


def get_checkpointer() -> FileCheckpointer:
    global _checkpointer
    if _checkpointer is None:
        _checkpointer = FileCheckpointer(
            CheckpointConfig(base_dir=Path(".sixth") / "checkpoints", ttl_hours=24)
        )
    return _checkpointer


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def create_session(username: str, name: str, is_admin: bool = False) -> dict[str, Any]:
    now = _utcnow()
    # Faxina oportunista: nenhum cron dedicado, então aproveita o login de
    # alguém para descartar sessões vencidas em vez de deixá-las acumular.
    delete_expired_sessions(now.isoformat())

    token = str(uuid.uuid4())
    expires = now + timedelta(hours=int(get_runtime_config("SESSION_TTL_HOURS", "8")))
    login_at = now.isoformat()
    create_session_row(token, username, name, is_admin, expires.isoformat(), login_at)

    return {
        "token": token,
        "username": username,
        "name": name,
        "is_admin": is_admin,
        "expires": expires,
        "login_at": login_at,
    }


def extract_bearer_token(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Nao autenticado.")
    return authorization.split(" ", 1)[1]


def get_current_user(authorization: str = Header(default=None)) -> dict[str, Any]:
    token = extract_bearer_token(authorization)
    row = get_session_row(token)

    if not row:
        raise HTTPException(status_code=401, detail="Sessao invalida ou expirada.")

    expires = datetime.fromisoformat(row["expires_at"])
    if _utcnow() > expires:
        delete_session_row(token)
        raise HTTPException(status_code=401, detail="Sessao expirada. Faca login novamente.")

    return {
        "token": row["token"],
        "username": row["username"],
        "name": row["name"],
        "is_admin": bool(row["is_admin"]),
        "expires": expires,
        "login_at": row["login_at"],
    }


def remove_current_session(authorization: str | None) -> None:
    token = extract_bearer_token(authorization)
    delete_session_row(token)


def session_count() -> int:
    return count_sessions()


def _is_bcrypt_hash(value: str) -> bool:
    return value.startswith("$2a$") or value.startswith("$2b$") or value.startswith("$2y$")


def verify_password(plain_password: str, stored_password: str) -> bool:
    if not stored_password:
        return False

    if _is_bcrypt_hash(stored_password):
        try:
            return bcrypt.checkpw(
                plain_password.encode("utf-8"),
                stored_password.encode("utf-8"),
            )
        except Exception:
            return False

    return plain_password == stored_password


def load_users() -> dict[str, dict[str, str]]:
    from src.core.database import get_user as _get_user, list_users as _list_users

    db_users = _list_users()
    return {
        u["username"]: {
            "password": _get_user(u["username"])["password_hash"],
            "name": u["name"],
            "is_admin": str(u["is_admin"]),
        }
        for u in db_users
    }


def get_admin_user(session: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
    if not session.get("is_admin"):
        raise HTTPException(status_code=403, detail="Acesso restrito a administradores.")
    return session
