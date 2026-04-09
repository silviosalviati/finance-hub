from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from src.api.dependencies import (
    create_session,
    get_current_user,
    load_users,
    remove_current_session,
    verify_password,
)
from src.shared.config import SESSION_TTL_HOURS

router = APIRouter(prefix="/api", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
async def login(req: LoginRequest):
    users = load_users()
    username = req.username.strip()
    user_data = users.get(username)

    if not user_data or not verify_password(req.password, user_data["password"]):
        raise HTTPException(status_code=401, detail="Matricula ou senha incorretos.")

    session = create_session(username=username, name=user_data["name"])

    return {
        "token": session["token"],
        "username": session["username"],
        "name": session["name"],
        "expires_in_hours": SESSION_TTL_HOURS,
    }


@router.post("/logout")
async def logout(
    _session: dict[str, Any] = Depends(get_current_user),
    authorization: str = Header(default=None),
):
    remove_current_session(authorization)
    return {"ok": True}


@router.get("/me")
async def me(session: dict[str, Any] = Depends(get_current_user)):
    return {
        "username": session["username"],
        "name": session["name"],
        "login_at": session["login_at"],
    }
