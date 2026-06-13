# Admin Panel — Design & Implementation

**Date:** 2026-06-13  
**Status:** Implemented

## Problem

All user accounts and runtime configuration were stored in environment variables (`.env`). Adding a new user or changing a parameter required editing the file and restarting the server. There was no UI for these operations.

## Solution

Persist users and runtime config in SQLite (`.sixth/app.db`) and expose an admin-only panel in the existing frontend.

---

## What stays in `.env`

Only GCP/Vertex AI credentials (cannot be changed at runtime):

| Variable | Purpose |
|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | Service account key path |
| `GCP_PROJECT_ID` | GCP project for BigQuery |
| `VERTEXAI_PROJECT` | Vertex AI project |
| `VERTEXAI_LOCATION` | Vertex AI region |
| `HF_API_TOKEN` | Hugging Face token |

---

## Database schema (`.sixth/app.db`)

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,  -- bcrypt
    name TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE app_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    updated_by TEXT NOT NULL DEFAULT 'system'
);
```

**Config keys migrated from `.env`:**
`VERTEXAI_MODEL`, `VERTEXAI_MAX_OUTPUT_TOKENS`, `VERTEXAI_MAX_RETRIES`, `VERTEXAI_TEMPERATURE`, `SESSION_TTL_HOURS`, `BQ_COST_PER_TB_USD`, `BYTES_WARNING_THRESHOLD`, `BYTES_CRITICAL_THRESHOLD`, `ALLOWED_ORIGINS`

**Migration:** On first startup (`init_db()`), if tables are empty, seeds from current `.env` values. The first user in `APP_USERS` gets `is_admin=1`. Subsequent startups ignore `.env` for these values.

---

## API routes

All routes under `/admin` require a valid session with `is_admin=True` (HTTP 403 otherwise).

| Method | Route | Description |
|---|---|---|
| GET | `/admin/users` | List all users (no password) |
| POST | `/admin/users` | Create user |
| PUT | `/admin/users/{username}` | Update name / password / is_admin |
| DELETE | `/admin/users/{username}` | Delete user (cannot delete self) |
| GET | `/admin/config` | List all config params |
| PUT | `/admin/config/{key}` | Update a config value |

Login (`POST /api/login`) now returns `is_admin: bool`.

---

## Frontend

- Admin navigation section (Users + Parâmetros) hidden for non-admin users via `currentUser.is_admin`.
- **Users panel:** table with edit/delete actions + modal for create/edit.
- **Config panel:** table with inline editable inputs + per-row save button.
- No external JS dependencies added.

---

## Files changed

| File | Change |
|---|---|
| `src/core/database.py` | **New** — SQLite CRUD, init_db, seed migration |
| `src/api/routes/admin.py` | **New** — admin REST routes |
| `src/api/dependencies.py` | `load_users` reads SQLite; `create_session` carries `is_admin`; `get_admin_user` dep added |
| `src/api/routes/auth.py` | Login + `/me` return `is_admin` |
| `src/api/main.py` | `init_db()` in lifespan; admin router registered |
| `src/shared/config.py` | `get_runtime_config()` helper added |
| `static/index.html` | Admin nav section + Users view + Config view + user modal |
| `static/js/scripts.js` | `doLogin` stores `is_admin`; `navTo` handles admin views; admin CRUD functions |
| `static/css/style.css` | Admin table, modal, badge, button styles |
