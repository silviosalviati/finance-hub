from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel

from src.api.dependencies import get_checkpointer, get_current_user, get_registry
from src.shared.tools.llm import create_llm
from src.shared.tools.bigquery import (
    validate_dataset_for_query_build,
    validate_query_context_for_query_analyzer,
)

router = APIRouter(tags=["agents"])


class AnalyzeRequest(BaseModel):
    query: str
    project_id: str | None = None
    dataset_hint: str | None = None


class ValidateDatasetRequest(BaseModel):
    project_id: str
    dataset_hint: str


class ValidateAnalyzerContextRequest(BaseModel):
    query: str
    project_id: str | None = None


_CHAT_LLM = None
_DEFAULT_FINANCE_PROJECT = "silviosalviati"
_NAME_PATTERNS = (
    re.compile(r"\bmeu nome\s*(?:é|e)\s*([a-zA-ZÀ-ÿ][\wÀ-ÿ\- ]{0,40})", re.IGNORECASE),
    re.compile(r"\bme chamo\s+([a-zA-ZÀ-ÿ][\wÀ-ÿ\- ]{0,40})", re.IGNORECASE),
)
_ASK_NAME_PATTERN = re.compile(
    r"\b(qual(?:\s*(?:é|e))?\s*meu\s*nome|lembra\s*meu\s*nome|sabe\s*meu\s*nome)\b",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-ZÀ-ÿ0-9_]+", text.lower())
        if len(token) > 2
    }


def _is_analytics_query(query: str) -> bool:
    q = _normalize_text(query)
    analytics_terms = (
        "analise",
        "análise",
        "relatorio",
        "relatório",
        "friccao",
        "fricção",
        "sentimento",
        "tema",
        "temas",
        "voc",
        "periodo",
        "período",
        "ultimos",
        "últimos",
        "mes",
        "mês",
        "atendimento",
        "atendimentos",
    )
    return any(term in q for term in analytics_terms)


def _extract_user_name(query: str) -> str | None:
    for pattern in _NAME_PATTERNS:
        match = pattern.search(query)
        if not match:
            continue
        candidate = re.sub(r"\s+", " ", match.group(1)).strip(" .,!?:;")
        if candidate:
            return candidate
    return None


def _is_asking_name(query: str) -> bool:
    return bool(_ASK_NAME_PATTERN.search(query))


def _load_finance_chat_session(token: str, checkpointer) -> dict[str, Any]:
    key = f"{token}-finance_auditor-chat"
    payload = checkpointer.load(key)
    if isinstance(payload, dict):
        payload.setdefault("turns", [])
        payload.setdefault("profile", {})
        return payload
    return {
        "created_at": _now_iso(),
        "profile": {},
        "turns": [],
    }


def _save_finance_chat_session(token: str, checkpointer, session_payload: dict[str, Any]) -> None:
    key = f"{token}-finance_auditor-chat"
    checkpointer.save(key, session_payload)


def _find_repeated_response(turns: list[dict[str, Any]], query_norm: str) -> dict[str, Any] | None:
    for turn in reversed(turns):
        if turn.get("query_norm") != query_norm:
            continue
        response = turn.get("response")
        if isinstance(response, dict):
            reused = dict(response)
            reused.setdefault("warnings", [])
            reused["warnings"] = [
                *reused.get("warnings", []),
                "Pergunta repetida detectada: reutilizando resposta anterior da sessão.",
            ]
            reused["response_reused"] = True
            return reused
    return None


def _retrieve_relevant_turns(turns: list[dict[str, Any]], query: str, top_k: int = 4) -> list[dict[str, Any]]:
    query_tokens = _tokenize(query)
    if not query_tokens:
        return []

    scored: list[tuple[int, dict[str, Any]]] = []
    for turn in turns:
        text = f"{turn.get('query', '')} {turn.get('answer_text', '')}"
        tokens = _tokenize(text)
        score = len(query_tokens.intersection(tokens))
        if score > 0:
            scored.append((score, turn))

    scored.sort(key=lambda item: item[0], reverse=True)
    return [turn for _, turn in scored[:top_k]]


def _llm_for_chat():
    global _CHAT_LLM
    if _CHAT_LLM is None:
        _CHAT_LLM = create_llm()
    return _CHAT_LLM


def _llm_text(response: Any) -> str:
    if hasattr(response, "content"):
        return str(response.content)
    return str(response)


def _build_rag_chat_answer(query: str, profile: dict[str, Any], relevant_turns: list[dict[str, Any]]) -> str:
    context_lines: list[str] = []
    remembered_name = str(profile.get("name") or "").strip()
    if remembered_name:
        context_lines.append(f"- Nome informado pelo usuário: {remembered_name}")

    for idx, turn in enumerate(relevant_turns, start=1):
        context_lines.append(f"- Turno {idx} pergunta: {turn.get('query', '')}")
        answer = str(turn.get("answer_text", "")).strip()
        if answer:
            context_lines.append(f"  resposta: {answer}")

    if not context_lines:
        context_lines.append("- Sem contexto anterior relevante na sessão.")

    try:
        llm = _llm_for_chat()
        response = llm.invoke(
            [
                SystemMessage(
                    content=(
                        "Você é o Finance Voice IA em modo conversacional. "
                        "Responda em português, de forma objetiva e útil. "
                        "NÃO gere relatório VoC nesta resposta. "
                        "Quando houver memória de sessão, use-a para responder." 
                    )
                ),
                HumanMessage(
                    content=(
                        f"Pergunta atual do usuário: {query}\n\n"
                        "Contexto recuperado (RAG lexical da sessão):\n"
                        f"{'\n'.join(context_lines)}\n\n"
                        "Responda em até 5 linhas."
                    )
                ),
            ]
        )
        text = _llm_text(response).strip()
        if text:
            return text
    except Exception:
        pass

    # Fallback determinístico quando o LLM falhar
    if remembered_name and _is_asking_name(query):
        return f"Seu nome é {remembered_name}."

    return (
        "Posso te ajudar com perguntas da sessão e também com análises VoC. "
        "Se quiser relatório, peça algo como: 'analise os atendimentos do mês passado'."
    )


def _build_finance_chat_response(answer: str, warnings: list[str] | None = None) -> dict[str, Any]:
    return {
        "status": "ok",
        "response_mode": "chat",
        "chat_answer": answer,
        "warnings": warnings or [],
    }


@router.get("/api/runtime-llm")
async def runtime_llm_info():
    registry = get_registry()
    agent = registry.get("query_analyzer")
    return agent.runtime_info()


@router.get("/api/agents")
async def list_agents():
    registry = get_registry()
    return {"agents": registry.list_ids()}


@router.post("/api/agents/{agent_id}/analyze")
async def analyze_by_agent(
    agent_id: str,
    req: AnalyzeRequest,
    session: dict[str, Any] = Depends(get_current_user),
):
    query = req.query.strip()
    project_id = req.project_id.strip() if req.project_id else ""

    if not query:
        raise HTTPException(status_code=400, detail="Query nao pode ser vazia.")
    if agent_id not in {"query_analyzer", "finance_auditor"} and not project_id:
        raise HTTPException(status_code=400, detail="Project ID nao pode ser vazio.")
    if agent_id == "finance_auditor" and not project_id:
        project_id = _DEFAULT_FINANCE_PROJECT

    registry = get_registry()
    try:
        agent = registry.get(agent_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        checkpointer = get_checkpointer()

        # Modo conversacional com memória de sessão + RAG lexical
        if agent_id == "finance_auditor":
            chat_session = _load_finance_chat_session(session["token"], checkpointer)
            turns: list[dict[str, Any]] = chat_session.get("turns", [])
            profile: dict[str, Any] = chat_session.get("profile", {})

            query_norm = _normalize_text(query)
            repeated = _find_repeated_response(turns, query_norm)
            if repeated is not None:
                checkpoint_key = f"{session['token']}-{agent_id}"
                checkpointer.save(checkpoint_key, repeated)
                return repeated

            remembered_name = _extract_user_name(query)
            if remembered_name:
                profile["name"] = remembered_name
                response = _build_finance_chat_response(
                    f"Perfeito, vou lembrar. Seu nome é {remembered_name}."
                )
            elif _is_asking_name(query):
                name = str(profile.get("name") or "").strip()
                if name:
                    response = _build_finance_chat_response(f"Seu nome é {name}.")
                else:
                    response = _build_finance_chat_response(
                        "Ainda não tenho seu nome salvo nesta sessão. "
                        "Pode me dizer algo como: 'meu nome é João'."
                    )
            elif not _is_analytics_query(query):
                relevant_turns = _retrieve_relevant_turns(turns, query)
                answer = _build_rag_chat_answer(query, profile, relevant_turns)
                response = _build_finance_chat_response(answer)
            else:
                response = agent.analyze(
                    query=query,
                    project_id=project_id,
                    dataset_hint=req.dataset_hint,
                )
                response.setdefault("response_mode", "analysis")

            turns.append(
                {
                    "at": _now_iso(),
                    "query": query,
                    "query_norm": query_norm,
                    "mode": response.get("response_mode", "analysis"),
                    "answer_text": (
                        response.get("chat_answer")
                        or response.get("markdown_report")
                        or response.get("error")
                        or ""
                    )[:800],
                    "response": response,
                }
            )
            # Janela curta para evitar crescimento indefinido
            chat_session["turns"] = turns[-40:]
            chat_session["profile"] = profile
            chat_session["updated_at"] = _now_iso()
            _save_finance_chat_session(session["token"], checkpointer, chat_session)

            checkpoint_key = f"{session['token']}-{agent_id}"
            checkpointer.save(checkpoint_key, response)
            return response

        result = agent.analyze(query=query, project_id=project_id, dataset_hint=req.dataset_hint)
        checkpoint_key = f"{session['token']}-{agent_id}"
        checkpointer.save(checkpoint_key, result)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/agents/query_build/validate-dataset")
async def validate_query_build_dataset(
    req: ValidateDatasetRequest,
    _session: dict[str, Any] = Depends(get_current_user),
):
    project_id = req.project_id.strip()
    dataset_hint = req.dataset_hint.strip()

    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID nao pode ser vazio.")
    if not dataset_hint:
        raise HTTPException(status_code=400, detail="Dataset hint nao pode ser vazio.")

    try:
        return validate_dataset_for_query_build(
            project_id=project_id,
            dataset_hint=dataset_hint,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/api/agents/query_analyzer/validate-query-context")
async def validate_query_analyzer_context(
    req: ValidateAnalyzerContextRequest,
    _session: dict[str, Any] = Depends(get_current_user),
):
    query = req.query.strip()
    project_id = req.project_id.strip() if req.project_id else None

    if not query:
        raise HTTPException(status_code=400, detail="Query nao pode ser vazia.")

    try:
        return validate_query_context_for_query_analyzer(
            query=query,
            project_id=project_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/api/agents/{agent_id}/checkpoint")
async def get_agent_checkpoint(
    agent_id: str,
    session: dict[str, Any] = Depends(get_current_user),
):
    checkpoint_key = f"{session['token']}-{agent_id}"
    payload = get_checkpointer().load(checkpoint_key)
    if payload is None:
        return {"status": "empty", "checkpoint": None}
    return {"status": "ok", "checkpoint": payload}
