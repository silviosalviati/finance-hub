from __future__ import annotations

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

from src.api.dependencies import session_count
from src.api.routes.admin import router as admin_router
from src.api.routes.agents import router as agents_router
from src.api.routes.auth import router as auth_router
from src.api.routes.finance_governance import router as finance_governance_router
from src.api.routes.schema_explorer import router as schema_explorer_router
from src.core.database import init_db
from src.shared.config import ALLOWED_ORIGINS, LLM_PROVIDER, validate_runtime_config
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
    print(f"LLM_PROVIDER: {LLM_PROVIDER}")
    print(f"ALLOWED_ORIGINS: {ALLOWED_ORIGINS}")
    yield


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
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
