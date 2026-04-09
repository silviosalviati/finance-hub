from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.api.dependencies import session_count
from src.api.routes.agents import router as agents_router
from src.api.routes.auth import router as auth_router
from src.shared.config import ALLOWED_ORIGINS, print_runtime_summary, validate_runtime_config

app = FastAPI(title="Finance Hub IA", version="3.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(auth_router)
app.include_router(agents_router)


def _validate_startup_config() -> None:
    errors = validate_runtime_config()
    if errors:
        raise RuntimeError("Configuracao invalida:\n- " + "\n- ".join(errors))


def _portal_html_path() -> Path:
    return Path(__file__).resolve().parents[2] / "static" / "index.html"


@app.on_event("startup")
def startup_event() -> None:
    _validate_startup_config()
    print_runtime_summary()


@app.get("/", response_class=HTMLResponse)
async def serve_portal():
    html_path = _portal_html_path()
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="Portal nao encontrado.")
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
        raise HTTPException(status_code=404, detail="Favicon nao encontrado.")
    return FileResponse(favicon_path, media_type="image/png")


if __name__ == "__main__":
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
