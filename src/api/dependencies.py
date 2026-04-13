from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import bcrypt
from fastapi import Header, HTTPException

from src.agents.document_build import DocumentBuildAgent
from src.agents.finance_auditor import FinanceAuditorAgent
from src.agents.query_analyzer import QueryAnalyzerAgent
from src.agents.query_build import QueryBuildAgent
from src.core.checkpointer import CheckpointConfig, FileCheckpointer
from src.core.registry import AgentRegistry
from src.shared.config import SESSION_TTL_HOURS

_sessions: dict[str, dict[str, Any]] = {}
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
    return datetime.utcnow()


def create_session(username: str, name: str) -> dict[str, Any]:
    token = str(uuid.uuid4())
    session = {
        "token": token,
        "username": username,
        "name": name,
        "expires": _utcnow() + timedelta(hours=SESSION_TTL_HOURS),
        "login_at": _utcnow().isoformat(),
    }
    _sessions[token] = session
    return session


def extract_bearer_token(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Nao autenticado.")
    return authorization.split(" ", 1)[1]


def get_current_user(authorization: str = Header(default=None)) -> dict[str, Any]:
    token = extract_bearer_token(authorization)
    session = _sessions.get(token)

    if not session:
        raise HTTPException(status_code=401, detail="Sessao invalida ou expirada.")

    if _utcnow() > session["expires"]:
        _sessions.pop(token, None)
        raise HTTPException(status_code=401, detail="Sessao expirada. Faca login novamente.")

    return session


def remove_current_session(authorization: str | None) -> None:
    token = extract_bearer_token(authorization)
    _sessions.pop(token, None)


def session_count() -> int:
    return len(_sessions)


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
    users: dict[str, dict[str, str]] = {}

    raw = os.getenv("APP_USERS", "").strip()
    if raw:
        for entry in raw.split(","):
            parts = entry.strip().split(":", 2)
            if len(parts) >= 2:
                username = parts[0].strip()
                password = parts[1].strip()
                name = parts[2].strip() if len(parts) == 3 else username.title()
                if username:
                    users[username] = {"password": password, "name": name}

    if users:
        return users

    username = os.getenv("APP_USERNAME", "admin").strip()
    password = os.getenv("APP_PASSWORD", "porto2024").strip()
    name = os.getenv("APP_NAME", "Administrador").strip()

    users[username] = {"password": password, "name": name}
    return users
