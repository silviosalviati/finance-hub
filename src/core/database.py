from __future__ import annotations

import json
import os
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import bcrypt

_DB_PATH = Path(".sixth") / "app.db"

_CONFIG_DEFAULTS: dict[str, tuple[str, str]] = {
    # LLM / Vertex AI
    "LLM_PROVIDER": ("vertexai", "Provedor LLM (vertexai)"),
    "VERTEXAI_PROJECT": ("", "Projeto Vertex AI (vazio = descobre a partir das credenciais)"),
    "VERTEXAI_LOCATION": ("us-central1", "Região Vertex AI"),
    "VERTEXAI_MODEL": ("gemini-2.5-flash", "Modelo Vertex AI / Gemini"),
    "VERTEXAI_MAX_OUTPUT_TOKENS": ("8192", "Máximo de tokens de saída do LLM"),
    "VERTEXAI_MAX_RETRIES": ("1", "Tentativas de retry do Vertex AI SDK"),
    "VERTEXAI_TEMPERATURE": ("0.05", "Temperatura analítica do LLM — análise e otimização (0.0 – 1.0)"),
    "VERTEXAI_TEMPERATURE_CREATIVE": ("0.3", "Temperatura criativa do LLM — relatórios e temas (0.0 – 1.0)"),
    "FINANCE_AUDITOR_LITE_MODEL": (
        "",
        "Modelo mais barato/rápido para tarefas simples do Finance Auditor "
        "(seleção de tabelas, veredito do Reflect) — vazio = usa o mesmo "
        "modelo de tudo (VERTEXAI_MODEL), sem tiering. Ex.: gemini-2.5-flash-lite",
    ),
    # GCP / BigQuery
    "GCP_PROJECT_ID": ("", "IDs de projetos GCP permitidos, separados por vírgula (vazio = descobre a partir das credenciais)"),
    "GOOGLE_APPLICATION_CREDENTIALS": (
        "secrets/credentials.json",
        "Caminho do arquivo de credenciais GCP (relativo à raiz do projeto)",
    ),
    "BQ_COST_PER_TB_USD": ("5.0", "Custo por TB processado no BigQuery (USD)"),
    "BYTES_WARNING_THRESHOLD": ("10737418240", "Limite de alerta de bytes (10 GB)"),
    "BYTES_CRITICAL_THRESHOLD": ("107374182400", "Limite crítico de bytes (100 GB)"),
    # Observabilidade / LangSmith
    "LANGCHAIN_API_KEY": ("", "API key do LangSmith para tracing (vazio = desativado)"),
    "LANGCHAIN_PROJECT": ("finance-hub", "Nome do projeto no LangSmith"),
    # Query Analyzer
    "QA_MAX_ITERATIONS": ("2", "Número máximo de iterações de otimização do Query Analyzer"),
    # Query Builder
    "QUERY_BUILD_BUDGET_BYTES": (
        "5368709120", "Budget máximo (bytes) por query gerada pelo Query Builder — 5 GiB"
    ),
    "QUERY_BUILD_MIN_QUALITY_SCORE": (
        "80", "Nota mínima (0-100) de boas práticas antes de pedir aprovação humana"
    ),
    # Finance Voice IA — governança (Fase 3)
    "FINANCE_AUDITOR_PII_MODE": (
        "mask", "Modo do PII Guard: mask | block | off"
    ),
    "FINANCE_AUDITOR_RBAC_STRICT": (
        "1", "RBAC strict: '1' bloqueia usuários sem ACL configurada"
    ),
    "FINANCE_AUDITOR_QUERY_BUDGET_BYTES": (
        "5368709120", "Budget máximo (bytes) por query — 5 GiB"
    ),
    "FINANCE_AUDITOR_TOKEN_BUDGET": (
        "80000", "Budget máximo de tokens de LLM (entrada+saída) por requisição — corta chamadas adicionais além disso"
    ),
    "FINANCE_AUDITOR_DEFAULT_DATASET": (
        "", "Dataset padrão para bq_list_tables quando não informado"
    ),
    "FINANCE_AUDITOR_TTS_VOICE": (
        "pt-BR-Chirp3-HD-Achernar", "Voz padrão do podcast do Finance Voice"
    ),
    "FINANCE_AUDITOR_TTS_VOICE_MASCULINA": (
        "", "Voz do podcast para gênero masculino (vazio = padrão)"
    ),
    "FINANCE_AUDITOR_TTS_VOICE_FEMININA": (
        "", "Voz do podcast para gênero feminino (vazio = padrão)"
    ),
    "FINANCE_AUDITOR_TTS_SPEAKING_RATE": (
        "1.0", "Velocidade de fala do podcast do Finance Voice"
    ),
    "FINANCE_AUDITOR_PODCAST_TTL_HOURS": (
        "24", "Tempo de vida dos arquivos de podcast em horas"
    ),
    "FINANCE_AUDITOR_PODCAST_MAX_BYTES": (
        "20000000", "Tamanho máximo do áudio do podcast em bytes"
    ),
    # App
    "SESSION_TTL_HOURS": ("8", "Tempo de vida da sessão em horas"),
    "ALLOWED_ORIGINS": (
        "http://localhost:8000,http://127.0.0.1:8000",
        "Origens CORS permitidas (separadas por vírgula)",
    ),
}

_DEFAULT_ADMIN_PASS = os.getenv("ADMIN_DEFAULT_PASSWORD") or secrets.token_urlsafe(16)
_DEFAULT_ADMIN_USER = ("admin", _DEFAULT_ADMIN_PASS, "Administrador", True)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def get_db():
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
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
                gerencia TEXT NOT NULL DEFAULT '',
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

            CREATE TABLE IF NOT EXISTS query_analyzer_memory (
                project_dataset TEXT PRIMARY KEY,
                patterns TEXT NOT NULL DEFAULT '',
                analysis_count INTEGER NOT NULL DEFAULT 0,
                last_updated TEXT NOT NULL
            );

            -- Fase 3 do Finance Voice IA: Semantic Layer (sem métricas pré-cadastradas).
            CREATE TABLE IF NOT EXISTS finance_semantic_metrics (
                key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                source_table TEXT NOT NULL DEFAULT '',
                sql_template TEXT NOT NULL,
                owner TEXT NOT NULL DEFAULT '',
                tags TEXT NOT NULL DEFAULT '',
                alert_threshold TEXT NOT NULL DEFAULT '',
                domain TEXT NOT NULL DEFAULT '',
                is_official INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            -- Fase 4: memória organizacional (fatos persistentes por usuário).
            CREATE TABLE IF NOT EXISTS finance_org_facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT '',
                scope TEXT NOT NULL DEFAULT 'user',  -- 'user' | 'global'
                fact_text TEXT NOT NULL,
                tags TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_finance_org_facts_user
                ON finance_org_facts (user_id, id DESC);

            -- Fase 3: RBAC por usuário (datasets e métricas permitidos).
            CREATE TABLE IF NOT EXISTS finance_user_acl (
                user_id TEXT PRIMARY KEY,
                allowed_datasets TEXT NOT NULL DEFAULT '',
                allowed_metrics TEXT NOT NULL DEFAULT '',
                denied_datasets TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );

            -- Fase 3: Audit trail das execuções do Finance Voice IA.
            CREATE TABLE IF NOT EXISTS finance_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                user_id TEXT NOT NULL DEFAULT '',
                persona TEXT NOT NULL DEFAULT '',
                request_text TEXT NOT NULL DEFAULT '',
                plan_json TEXT NOT NULL DEFAULT '[]',
                steps_total INTEGER NOT NULL DEFAULT 0,
                steps_ok INTEGER NOT NULL DEFAULT 0,
                bytes_processed INTEGER NOT NULL DEFAULT 0,
                estimated_cost_usd REAL NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                token_usage_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_finance_audit_log_user_ts
                ON finance_audit_log (user_id, ts DESC);

            -- Assets de podcast gerados a partir de análises anteriores.
            CREATE TABLE IF NOT EXISTS finance_podcast_assets (
                asset_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL DEFAULT '',
                user_id TEXT NOT NULL DEFAULT '',
                audit_id INTEGER NOT NULL DEFAULT 0,
                title TEXT NOT NULL DEFAULT '',
                mime_type TEXT NOT NULL DEFAULT 'audio/mpeg',
                audio_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_finance_podcast_assets_thread
                ON finance_podcast_assets (thread_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_finance_podcast_assets_audit
                ON finance_podcast_assets (audit_id);
            CREATE INDEX IF NOT EXISTS idx_finance_podcast_assets_expires
                ON finance_podcast_assets (expires_at);

            -- RAG do catálogo: índice semântico de datasets/tabelas/colunas
            -- para o Planner achar dados por significado, não por nome.
            CREATE TABLE IF NOT EXISTS finance_catalog_index (
                project_id TEXT NOT NULL,
                dataset_id TEXT NOT NULL,
                table_id TEXT NOT NULL,
                full_name TEXT NOT NULL,
                text_summary TEXT NOT NULL DEFAULT '',
                embedding_json TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (project_id, dataset_id, table_id)
            );
            CREATE INDEX IF NOT EXISTS idx_finance_catalog_index_project
                ON finance_catalog_index (project_id);

            -- Sessões de login — persistidas para sobreviver a reload e para
            -- serem visíveis por qualquer worker/processo (multi-worker).
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                name TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                gerencia TEXT NOT NULL DEFAULT '',
                expires_at TEXT NOT NULL,
                login_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_expires
                ON sessions (expires_at);
        """)

        _seed_if_empty(conn)
        _ensure_config_keys(conn)
        _prune_stale_config_keys(conn)
        _migrate_finance_metrics_columns(conn)
        _migrate_user_columns(conn)
        _migrate_audit_log_columns(conn)
        _migrate_podcast_asset_columns(conn)


def _migrate_audit_log_columns(conn: sqlite3.Connection) -> None:
    """Adiciona a coluna token_usage_json em finance_audit_log quando ausente (bancos antigos)."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(finance_audit_log)")}
    if "token_usage_json" not in cols:
        conn.execute(
            "ALTER TABLE finance_audit_log ADD COLUMN token_usage_json TEXT NOT NULL DEFAULT '{}'"
        )


def _migrate_podcast_asset_columns(conn: sqlite3.Connection) -> None:
    """Garante a tabela de assets de podcast em bancos antigos."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(finance_podcast_assets)")}
    if not cols:
        return
    if "thread_id" not in cols:
        conn.execute("ALTER TABLE finance_podcast_assets ADD COLUMN thread_id TEXT NOT NULL DEFAULT ''")
    if "user_id" not in cols:
        conn.execute("ALTER TABLE finance_podcast_assets ADD COLUMN user_id TEXT NOT NULL DEFAULT ''")
    if "audit_id" not in cols:
        conn.execute("ALTER TABLE finance_podcast_assets ADD COLUMN audit_id INTEGER NOT NULL DEFAULT 0")
    if "title" not in cols:
        conn.execute("ALTER TABLE finance_podcast_assets ADD COLUMN title TEXT NOT NULL DEFAULT ''")
    if "mime_type" not in cols:
        conn.execute("ALTER TABLE finance_podcast_assets ADD COLUMN mime_type TEXT NOT NULL DEFAULT 'audio/mpeg'")
    if "audio_path" not in cols:
        conn.execute("ALTER TABLE finance_podcast_assets ADD COLUMN audio_path TEXT NOT NULL DEFAULT ''")
    if "created_at" not in cols:
        conn.execute("ALTER TABLE finance_podcast_assets ADD COLUMN created_at TEXT NOT NULL DEFAULT ''")
    if "expires_at" not in cols:
        conn.execute("ALTER TABLE finance_podcast_assets ADD COLUMN expires_at TEXT NOT NULL DEFAULT ''")


def _migrate_user_columns(conn: sqlite3.Connection) -> None:
    """Adiciona a coluna gerencia em users/sessions quando ausente (bancos antigos)."""
    user_cols = {r[1] for r in conn.execute("PRAGMA table_info(users)")}
    if "gerencia" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN gerencia TEXT NOT NULL DEFAULT ''")

    session_cols = {r[1] for r in conn.execute("PRAGMA table_info(sessions)")}
    if "gerencia" not in session_cols:
        conn.execute("ALTER TABLE sessions ADD COLUMN gerencia TEXT NOT NULL DEFAULT ''")


def _migrate_finance_metrics_columns(conn: sqlite3.Connection) -> None:
    """Adiciona colunas novas em finance_semantic_metrics quando ausentes."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(finance_semantic_metrics)")}
    if "alert_threshold" not in cols:
        conn.execute(
            "ALTER TABLE finance_semantic_metrics ADD COLUMN"
            " alert_threshold TEXT NOT NULL DEFAULT ''"
        )
    if "domain" not in cols:
        conn.execute(
            "ALTER TABLE finance_semantic_metrics ADD COLUMN domain TEXT NOT NULL DEFAULT ''"
        )
    if "is_official" not in cols:
        conn.execute(
            "ALTER TABLE finance_semantic_metrics ADD COLUMN"
            " is_official INTEGER NOT NULL DEFAULT 0"
        )


# Chaves seedadas no mesmo dia em que a feature de podcast foi introduzida,
# nunca configuradas por um usuário real — substituídas pelo esquema de
# gênero (FINANCE_AUDITOR_TTS_VOICE_MASCULINA/FEMININA) antes de qualquer
# instalação chegar a usá-las de verdade.
_STALE_CONFIG_KEYS = (
    "FINANCE_AUDITOR_TTS_VOICE_COORDENADOR",
    "FINANCE_AUDITOR_TTS_VOICE_GERENTE",
    "FINANCE_AUDITOR_TTS_VOICE_DIRETOR",
    "FINANCE_AUDITOR_TTS_VOICE_GERAL",
)


def _prune_stale_config_keys(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "DELETE FROM app_config WHERE key = ?", [(key,) for key in _STALE_CONFIG_KEYS]
    )


def _seed_if_empty(conn: sqlite3.Connection) -> None:
    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    config_count = conn.execute("SELECT COUNT(*) FROM app_config").fetchone()[0]

    if user_count == 0:
        _seed_users_default(conn)

    if config_count == 0:
        _seed_config_defaults(conn)


def _ensure_config_keys(conn: sqlite3.Connection) -> None:
    """Add any new config keys added to _CONFIG_DEFAULTS that don't exist yet."""
    now = _utcnow()
    for key, (default, description) in _CONFIG_DEFAULTS.items():
        conn.execute(
            "INSERT OR IGNORE INTO app_config (key, value, description, updated_at, updated_by)"
            " VALUES (?, ?, ?, ?, 'system')",
            (key, default, description, now),
        )


def _seed_users_default(conn: sqlite3.Connection) -> None:
    now = _utcnow()
    username, password, name, is_admin = _DEFAULT_ADMIN_USER
    password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    conn.execute(
        "INSERT OR IGNORE INTO users (username, password_hash, name, is_admin, created_at, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (username, password_hash, name, 1 if is_admin else 0, now, now),
    )
    if not os.getenv("ADMIN_DEFAULT_PASSWORD"):
        print(
            f"[AVISO] Senha admin gerada automaticamente: {password!r} — "
            "altere via painel ou defina ADMIN_DEFAULT_PASSWORD no ambiente."
        )


def _seed_config_defaults(conn: sqlite3.Connection) -> None:
    now = _utcnow()
    for key, (default, description) in _CONFIG_DEFAULTS.items():
        conn.execute(
            "INSERT OR IGNORE INTO app_config (key, value, description, updated_at, updated_by)"
            " VALUES (?, ?, ?, ?, 'system')",
            (key, default, description, now),
        )


def _looks_like_bcrypt(value: str) -> bool:
    return value.startswith("$2a$") or value.startswith("$2b$") or value.startswith("$2y$")


# ── Sessions CRUD ────────────────────────────────────────────────────────────

def create_session_row(
    token: str,
    username: str,
    name: str,
    is_admin: bool,
    expires_at: str,
    login_at: str,
    gerencia: str = "",
) -> None:
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sessions (token, username, name, is_admin, gerencia, expires_at, login_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (token, username, name, 1 if is_admin else 0, gerencia, expires_at, login_at),
        )


def get_session_row(token: str) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT token, username, name, is_admin, gerencia, expires_at, login_at"
            " FROM sessions WHERE token = ?",
            (token,),
        ).fetchone()
    return dict(row) if row else None


def delete_session_row(token: str) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))


def delete_expired_sessions(now_iso: str) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM sessions WHERE expires_at < ?", (now_iso,))


def count_sessions() -> int:
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
    return int(row[0]) if row else 0


# ── Users CRUD ──────────────────────────────────────────────────────────────

def list_users() -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, username, name, is_admin, gerencia, created_at, updated_at FROM users ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def get_user(username: str) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, username, password_hash, name, is_admin, gerencia, created_at, updated_at"
            " FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    return dict(row) if row else None


def create_user(
    username: str, password: str, name: str, is_admin: bool, gerencia: str = ""
) -> dict[str, Any]:
    now = _utcnow()
    password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO users (username, password_hash, name, is_admin, gerencia, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (username, password_hash, name, 1 if is_admin else 0, gerencia, now, now),
        )
    return {"username": username, "name": name, "is_admin": is_admin, "gerencia": gerencia}


def update_user(
    username: str,
    *,
    name: str | None = None,
    password: str | None = None,
    is_admin: bool | None = None,
    gerencia: str | None = None,
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
    if gerencia is not None:
        sets.append("gerencia = ?")
        params.append(gerencia)

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


# ── Query Analyzer Memory ────────────────────────────────────────────────────

def get_dataset_memory(project_dataset: str) -> str:
    """Retorna memória cross-sessão como texto formatado para o LLM.

    Formato interno: JSON {"v": 2, "entries": [{...}]}
    Saída: linhas priorizadas por frequência e severidade.
    """
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT patterns FROM query_analyzer_memory WHERE project_dataset = ?",
                (project_dataset,),
            ).fetchone()
        if not row or not row["patterns"]:
            return ""
        raw = row["patterns"]
        # Tenta interpretar como JSON estruturado (v2)
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and data.get("v") == 2:
                entries = data.get("entries", [])
                _SEV_WEIGHT = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
                entries_sorted = sorted(
                    entries,
                    key=lambda e: (_SEV_WEIGHT.get(e.get("severity", "LOW"), 1) * 10 + e.get("count", 1)),
                    reverse=True,
                )
                lines = []
                for e in entries_sorted[:20]:
                    freq = f"x{e['count']}" if e.get("count", 1) > 1 else ""
                    lines.append(f"[{e.get('severity','?')}{freq}] {e.get('pattern','?')}: {e.get('suggestion','')}")
                return "\n".join(lines)
        except (json.JSONDecodeError, TypeError):
            pass
        # Fallback: formato texto legado
        return raw
    except Exception:
        return ""


def update_dataset_memory(project_dataset: str, new_entries: list[dict]) -> None:
    """Persiste antipadrões detectados com contagem de frequência.

    Args:
        project_dataset: chave "projeto:dataset"
        new_entries: lista de dicts com keys pattern, severity, suggestion
    """
    if not new_entries:
        return
    now = _utcnow()
    try:
        with get_db() as conn:
            existing_row = conn.execute(
                "SELECT patterns, analysis_count FROM query_analyzer_memory WHERE project_dataset = ?",
                (project_dataset,),
            ).fetchone()

            existing_entries: dict[str, dict] = {}
            analysis_count = 0

            if existing_row and existing_row["patterns"]:
                analysis_count = existing_row["analysis_count"] or 0
                try:
                    data = json.loads(existing_row["patterns"])
                    if isinstance(data, dict) and data.get("v") == 2:
                        for e in data.get("entries", []):
                            key = e.get("pattern", "").strip().lower()
                            if key:
                                existing_entries[key] = e
                except (json.JSONDecodeError, TypeError):
                    # Migra formato legado: cada linha "[SEV] pattern: suggestion"
                    for line in existing_row["patterns"].split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        # tenta parsear "[HIGH] SELECT *: sugestão"
                        import re as _re
                        m = _re.match(r"\[([A-Z]+)\]\s*(.+?):\s*(.+)", line)
                        if m:
                            sev, pat, sug = m.group(1), m.group(2).strip(), m.group(3).strip()
                            key = pat.lower()
                            existing_entries[key] = {"pattern": pat, "severity": sev, "suggestion": sug, "count": 1}

            # Merge: incrementa count se já existe, adiciona se novo
            for entry in new_entries:
                key = (entry.get("pattern") or "").strip().lower()
                if not key:
                    continue
                if key in existing_entries:
                    existing_entries[key]["count"] = existing_entries[key].get("count", 1) + 1
                    existing_entries[key]["last_seen"] = now
                else:
                    existing_entries[key] = {
                        "pattern": entry.get("pattern", "").strip(),
                        "severity": entry.get("severity", "MEDIUM"),
                        "suggestion": entry.get("suggestion", "").strip(),
                        "count": 1,
                        "last_seen": now,
                    }

            # Limita a 50 entradas (remove as menos frequentes)
            all_entries = sorted(existing_entries.values(), key=lambda e: e.get("count", 1), reverse=True)[:50]

            payload = json.dumps({"v": 2, "entries": all_entries}, ensure_ascii=False)

            if existing_row:
                conn.execute(
                    "UPDATE query_analyzer_memory SET patterns = ?, analysis_count = ?, last_updated = ?"
                    " WHERE project_dataset = ?",
                    (payload, analysis_count + 1, now, project_dataset),
                )
            else:
                conn.execute(
                    "INSERT INTO query_analyzer_memory (project_dataset, patterns, analysis_count, last_updated)"
                    " VALUES (?, ?, 1, ?)",
                    (project_dataset, payload, now),
                )
    except Exception:
        pass


# ── Finance Voice IA — Fase 3: Semantic Layer ───────────────────────────────

_METRIC_COLUMNS = (
    "key, name, description, source_table, sql_template, owner, tags,"
    " alert_threshold, domain, is_official, created_at, updated_at"
)


def list_finance_metrics() -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT {_METRIC_COLUMNS} FROM finance_semantic_metrics ORDER BY key"
        ).fetchall()
    return [_decode_metric_row(r) for r in rows]


def get_finance_metric(key: str) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(
            f"SELECT {_METRIC_COLUMNS} FROM finance_semantic_metrics WHERE key = ?",
            (key,),
        ).fetchone()
    return _decode_metric_row(row) if row else None


def _decode_metric_row(row: sqlite3.Row) -> dict[str, Any]:
    metric = dict(row)
    metric["is_official"] = bool(metric.get("is_official"))
    return metric


def upsert_finance_metric(
    key: str,
    *,
    name: str,
    description: str,
    source_table: str,
    sql_template: str,
    owner: str = "",
    tags: str = "",
    alert_threshold: str = "",
    domain: str = "",
    is_official: bool = False,
) -> dict[str, Any]:
    now = _utcnow()
    with get_db() as conn:
        existing = conn.execute(
            "SELECT created_at FROM finance_semantic_metrics WHERE key = ?", (key,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE finance_semantic_metrics SET name=?, description=?, source_table=?,"
                " sql_template=?, owner=?, tags=?, alert_threshold=?, domain=?, is_official=?,"
                " updated_at=? WHERE key=?",
                (name, description, source_table, sql_template, owner, tags,
                 alert_threshold, domain, int(bool(is_official)), now, key),
            )
            return {"key": key, "created": False, "updated_at": now}
        conn.execute(
            "INSERT INTO finance_semantic_metrics"
            " (key, name, description, source_table, sql_template, owner, tags,"
            "  alert_threshold, domain, is_official, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (key, name, description, source_table, sql_template, owner, tags,
             alert_threshold, domain, int(bool(is_official)), now, now),
        )
    return {"key": key, "created": True, "updated_at": now}


# ── Fase 4: memória organizacional ──────────────────────────────────────────

def insert_org_fact(*, user_id: str, fact_text: str, tags: str = "", scope: str = "user") -> int:
    now = _utcnow()
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO finance_org_facts (user_id, scope, fact_text, tags, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (user_id, scope, fact_text, tags, now),
        )
        return int(cur.lastrowid or 0)


def list_org_facts(
    user_id: str | None = None,
    include_global: bool = True,
    limit: int = 200,
) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit or 200), 1000))
    where: list[str] = []
    params: list[Any] = []
    if user_id:
        if include_global:
            where.append("(user_id = ? OR scope = 'global')")
            params.append(user_id)
        else:
            where.append("user_id = ?")
            params.append(user_id)
    elif not include_global:
        where.append("user_id <> ''")
    sql = "SELECT id, user_id, scope, fact_text, tags, created_at FROM finance_org_facts"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with get_db() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def delete_org_fact(fact_id: int) -> bool:
    with get_db() as conn:
        cur = conn.execute("DELETE FROM finance_org_facts WHERE id = ?", (int(fact_id),))
        return cur.rowcount > 0


def delete_finance_metric(key: str) -> bool:
    with get_db() as conn:
        cur = conn.execute("DELETE FROM finance_semantic_metrics WHERE key = ?", (key,))
        return cur.rowcount > 0


# ── Finance Voice IA — Fase 3: RBAC ─────────────────────────────────────────

def _split_csv(value: str) -> list[str]:
    return [v.strip() for v in (value or "").split(",") if v.strip()]


def _join_csv(items: list[str]) -> str:
    return ",".join(sorted({v.strip() for v in items if v and v.strip()}))


def get_finance_acl(user_id: str) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT user_id, allowed_datasets, allowed_metrics, denied_datasets, updated_at"
            " FROM finance_user_acl WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    if not row:
        return None
    data = dict(row)
    data["allowed_datasets"] = _split_csv(data["allowed_datasets"])
    data["allowed_metrics"] = _split_csv(data["allowed_metrics"])
    data["denied_datasets"] = _split_csv(data["denied_datasets"])
    return data


def upsert_finance_acl(
    user_id: str,
    *,
    allowed_datasets: list[str] | None = None,
    allowed_metrics: list[str] | None = None,
    denied_datasets: list[str] | None = None,
) -> dict[str, Any]:
    now = _utcnow()
    payload = (
        _join_csv(allowed_datasets or []),
        _join_csv(allowed_metrics or []),
        _join_csv(denied_datasets or []),
        now,
        user_id,
    )
    with get_db() as conn:
        existing = conn.execute(
            "SELECT 1 FROM finance_user_acl WHERE user_id = ?", (user_id,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE finance_user_acl SET allowed_datasets=?, allowed_metrics=?,"
                " denied_datasets=?, updated_at=? WHERE user_id=?",
                payload,
            )
        else:
            conn.execute(
                "INSERT INTO finance_user_acl"
                " (allowed_datasets, allowed_metrics, denied_datasets, updated_at, user_id)"
                " VALUES (?, ?, ?, ?, ?)",
                payload,
            )
    return {"user_id": user_id, "updated_at": now}


def list_finance_acl() -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT user_id, allowed_datasets, allowed_metrics, denied_datasets, updated_at"
            " FROM finance_user_acl ORDER BY user_id"
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d["allowed_datasets"] = _split_csv(d["allowed_datasets"])
        d["allowed_metrics"] = _split_csv(d["allowed_metrics"])
        d["denied_datasets"] = _split_csv(d["denied_datasets"])
        out.append(d)
    return out


# ── Finance Voice IA — Fase 3: Audit log ────────────────────────────────────

def append_finance_audit(entry: dict[str, Any]) -> int:
    now = entry.get("ts") or _utcnow()
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO finance_audit_log"
            " (ts, user_id, persona, request_text, plan_json, steps_total, steps_ok,"
            "  bytes_processed, estimated_cost_usd, error, token_usage_json)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                now,
                str(entry.get("user_id") or ""),
                str(entry.get("persona") or ""),
                str(entry.get("request_text") or "")[:4000],
                json.dumps(entry.get("plan") or [], ensure_ascii=False)[:8000],
                int(entry.get("steps_total") or 0),
                int(entry.get("steps_ok") or 0),
                int(entry.get("bytes_processed") or 0),
                float(entry.get("estimated_cost_usd") or 0.0),
                str(entry.get("error") or "")[:1000],
                json.dumps(entry.get("token_usage") or {}, ensure_ascii=False)[:4000],
            ),
        )
        return int(cur.lastrowid or 0)


def list_finance_audit(limit: int = 50, user_id: str | None = None) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit or 50), 500))
    with get_db() as conn:
        if user_id:
            rows = conn.execute(
                "SELECT * FROM finance_audit_log WHERE user_id = ? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM finance_audit_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [dict(r) for r in rows]


def upsert_finance_podcast_asset(entry: dict[str, Any]) -> None:
    with get_db() as conn:
        conn.execute(
            "INSERT INTO finance_podcast_assets"
            " (asset_id, thread_id, user_id, audit_id, title, mime_type, audio_path, created_at, expires_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(asset_id) DO UPDATE SET"
            "   thread_id = excluded.thread_id,"
            "   user_id = excluded.user_id,"
            "   audit_id = excluded.audit_id,"
            "   title = excluded.title,"
            "   mime_type = excluded.mime_type,"
            "   audio_path = excluded.audio_path,"
            "   created_at = excluded.created_at,"
            "   expires_at = excluded.expires_at",
            (
                str(entry.get("asset_id") or ""),
                str(entry.get("thread_id") or ""),
                str(entry.get("user_id") or ""),
                int(entry.get("audit_id") or 0),
                str(entry.get("title") or ""),
                str(entry.get("mime_type") or "audio/mpeg"),
                str(entry.get("audio_path") or ""),
                str(entry.get("created_at") or _utcnow()),
                str(entry.get("expires_at") or _utcnow()),
            ),
        )


def get_finance_podcast_asset(asset_id: str) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM finance_podcast_assets WHERE asset_id = ?",
            (asset_id,),
        ).fetchone()
    return dict(row) if row else None


def get_finance_podcast_asset_by_audit_id(audit_id: int) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM finance_podcast_assets WHERE audit_id = ? ORDER BY created_at DESC LIMIT 1",
            (int(audit_id),),
        ).fetchone()
    return dict(row) if row else None


def delete_expired_finance_podcast_assets(now_iso: str) -> int:
    with get_db() as conn:
        cur = conn.execute(
            "DELETE FROM finance_podcast_assets WHERE expires_at < ?",
            (now_iso,),
        )
        return int(cur.rowcount or 0)


# ── Finance Voice IA — RAG do catálogo (datasets/tabelas/colunas) ───────────

def upsert_catalog_entry(
    *,
    project_id: str,
    dataset_id: str,
    table_id: str,
    full_name: str,
    text_summary: str,
    embedding_json: str,
) -> None:
    now = _utcnow()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO finance_catalog_index"
            " (project_id, dataset_id, table_id, full_name, text_summary,"
            "  embedding_json, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(project_id, dataset_id, table_id) DO UPDATE SET"
            "   full_name = excluded.full_name,"
            "   text_summary = excluded.text_summary,"
            "   embedding_json = excluded.embedding_json,"
            "   updated_at = excluded.updated_at",
            (project_id, dataset_id, table_id, full_name, text_summary, embedding_json, now),
        )


def list_catalog_entries(project_id: str) -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM finance_catalog_index WHERE project_id = ? ORDER BY dataset_id, table_id",
            (project_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def delete_catalog_entries_for_dataset(project_id: str, dataset_id: str) -> None:
    with get_db() as conn:
        conn.execute(
            "DELETE FROM finance_catalog_index WHERE project_id = ? AND dataset_id = ?",
            (project_id, dataset_id),
        )


def get_catalog_oldest_update(project_id: str) -> str | None:
    """Data da entrada mais antiga do índice — usada para decidir TTL."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT MIN(updated_at) AS oldest FROM finance_catalog_index WHERE project_id = ?",
            (project_id,),
        ).fetchone()
    return row["oldest"] if row and row["oldest"] else None
