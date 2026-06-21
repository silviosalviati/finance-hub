from __future__ import annotations

import asyncio
import contextlib
import sys
from contextlib import asynccontextmanager
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from src.agents.finance_auditor.catalog_index import warmup_catalog_loop
from src.api.dependencies import session_count
from src.api.routes.admin import router as admin_router
from src.api.routes.agents import router as agents_router
from src.api.routes.auth import router as auth_router
from src.api.routes.finance_governance import router as finance_governance_router
from src.api.routes.schema_explorer import router as schema_explorer_router
from src.core.database import init_db
from src.shared.config import ALLOWED_ORIGINS, get_gcp_project_ids, validate_runtime_config
from src.shared.tracing import configure_tracing


def _validate_startup_config() -> None:
    errors = validate_runtime_config()
    if errors:
        raise RuntimeError("Configuração inválida:\n- " + "\n- ".join(errors))


def _portal_html_path() -> Path:
    return Path(__file__).resolve().parents[2] / "static" / "index.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    configure_tracing()
    _validate_startup_config()
    print(f"ALLOWED_ORIGINS: {ALLOWED_ORIGINS}")

    # Mantém o índice do catálogo do Finance Voice IA sempre quente, fora do
    # request de algum usuário (ver warmup_catalog_loop).
    warmup_task = asyncio.create_task(warmup_catalog_loop(get_gcp_project_ids()))
    try:
        yield
    finally:
        warmup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await warmup_task


app = FastAPI(title="Finance Hub IA", version="3.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(auth_router)
app.include_router(agents_router)
app.include_router(schema_explorer_router)
app.include_router(admin_router)
app.include_router(finance_governance_router)


@app.get("/", response_class=HTMLResponse)
async def serve_portal():
    html_path = _portal_html_path()
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="Portal não encontrado.")
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "sessions": session_count(),
    }


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    favicon_path = PROJECT_ROOT / "static" / "img" / "portoseguro.png"
    if not favicon_path.exists():
        raise HTTPException(status_code=404, detail="Favicon não encontrado.")
    return FileResponse(favicon_path, media_type="image/png")


if __name__ == "__main__":
    import os
    reload_enabled = os.getenv("UVICORN_RELOAD", "false").strip().lower() in ("1", "true", "yes")

    if reload_enabled:
        uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
    else:
        workers = int(os.getenv("UVICORN_WORKERS", "1"))
        uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, workers=workers)
