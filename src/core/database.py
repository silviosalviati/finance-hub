from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import bcrypt

_DB_PATH = Path(".sixth") / "app.db"

_CONFIG_DEFAULTS: dict[str, tuple[str, str]] = {
    "VERTEXAI_MODEL": ("gemini-2.5-flash", "Modelo Vertex AI / Gemini"),
    "VERTEXAI_MAX_OUTPUT_TOKENS": ("8192", "Máximo de tokens de saída do LLM"),
    "VERTEXAI_MAX_RETRIES": ("1", "Tentativas de retry do Vertex AI SDK"),
    "VERTEXAI_TEMPERATURE": ("0.05", "Temperatura do LLM (0.0 – 1.0)"),
    "SESSION_TTL_HOURS": ("8", "Tempo de vida da sessão em horas"),
    "BQ_COST_PER_TB_USD": ("5.0", "Custo por TB processado no BigQuery (USD)"),
    "BYTES_WARNING_THRESHOLD": ("10737418240", "Limite de alerta de bytes (10 GB)"),
    "BYTES_CRITICAL_THRESHOLD": ("107374182400", "Limite crítico de bytes (100 GB)"),
    "ALLOWED_ORIGINS": (
        "http://localhost:8000,http://127.0.0.1:8000",
        "Origens CORS permitidas (separadas por vírgula)",
    ),
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def get_db():
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                updated_by TEXT NOT NULL DEFAULT 'system'
            );
        """)

        _seed_if_empty(conn)


def _seed_if_empty(conn: sqlite3.Connection) -> None:
    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    config_count = conn.execute("SELECT COUNT(*) FROM app_config").fetchone()[0]

    if user_count == 0:
        _seed_users_from_env(conn)

    if config_count == 0:
        _seed_config_from_env(conn)


def _seed_users_from_env(conn: sqlite3.Connection) -> None:
    now = _utcnow()
    raw = os.getenv("APP_USERS", "").strip()
    entries: list[tuple[str, str, str]] = []

    if raw:
        for i, entry in enumerate(raw.split(",")):
            parts = entry.strip().split(":", 2)
            if len(parts) >= 2:
                username = parts[0].strip()
                password = parts[1].strip()
                name = parts[2].strip() if len(parts) == 3 else username.title()
                if username:
                    entries.append((username, password, name, i == 0))
    else:
        username = os.getenv("APP_USERNAME", "admin").strip()
        password = os.getenv("APP_PASSWORD", "porto2024").strip()
        name = os.getenv("APP_NAME", "Administrador").strip()
        entries.append((username, password, name, True))

    for username, password, name, is_first in entries:
        if not _looks_like_bcrypt(password):
            password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        conn.execute(
            "INSERT OR IGNORE INTO users (username, password_hash, name, is_admin, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (username, password, name, 1 if is_first else 0, now, now),
        )


def _seed_config_from_env(conn: sqlite3.Connection) -> None:
    now = _utcnow()
    for key, (default, description) in _CONFIG_DEFAULTS.items():
        value = os.getenv(key, default).strip() or default
        conn.execute(
            "INSERT OR IGNORE INTO app_config (key, value, description, updated_at, updated_by)"
            " VALUES (?, ?, ?, ?, 'system')",
            (key, value, description, now),
        )


def _looks_like_bcrypt(value: str) -> bool:
    return value.startswith("$2a$") or value.startswith("$2b$") or value.startswith("$2y$")


# ── Users CRUD ──────────────────────────────────────────────────────────────

def list_users() -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, username, name, is_admin, created_at, updated_at FROM users ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def get_user(username: str) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, username, password_hash, name, is_admin, created_at, updated_at"
            " FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    return dict(row) if row else None


def create_user(username: str, password: str, name: str, is_admin: bool) -> dict[str, Any]:
    now = _utcnow()
    password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO users (username, password_hash, name, is_admin, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (username, password_hash, name, 1 if is_admin else 0, now, now),
        )
    return {"username": username, "name": name, "is_admin": is_admin}


def update_user(
    username: str,
    *,
    name: str | None = None,
    password: str | None = None,
    is_admin: bool | None = None,
) -> bool:
    now = _utcnow()
    sets: list[str] = ["updated_at = ?"]
    params: list[Any] = [now]

    if name is not None:
        sets.append("name = ?")
        params.append(name)
    if password is not None:
        sets.append("password_hash = ?")
        params.append(bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode())
    if is_admin is not None:
        sets.append("is_admin = ?")
        params.append(1 if is_admin else 0)

    params.append(username)
    with get_db() as conn:
        cur = conn.execute(
            f"UPDATE users SET {', '.join(sets)} WHERE username = ?", params
        )
    return cur.rowcount > 0


def delete_user(username: str) -> bool:
    with get_db() as conn:
        cur = conn.execute("DELETE FROM users WHERE username = ?", (username,))
    return cur.rowcount > 0


# ── Config CRUD ─────────────────────────────────────────────────────────────

def get_config_all() -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT key, value, description, updated_at, updated_by FROM app_config ORDER BY key"
        ).fetchall()
    return [dict(r) for r in rows]


def get_config_value(key: str, default: str = "") -> str:
    with get_db() as conn:
        row = conn.execute("SELECT value FROM app_config WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_config_value(key: str, value: str, updated_by: str = "system") -> bool:
    now = _utcnow()
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE app_config SET value = ?, updated_at = ?, updated_by = ? WHERE key = ?",
            (value, now, updated_by, key),
        )
    return cur.rowcount > 0
