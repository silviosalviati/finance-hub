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


class SuggestionsRequest(BaseModel):
    project_id: str
    dataset_hint: str
    table_id: str


def _extract_known_schema_tokens(tables: list[dict[str, Any]]) -> tuple[set[str], set[str]]:
    known_tables: set[str] = set()
    known_columns: set[str] = set()

    for table in tables:
        table_name = str(table.get("table_name") or "").strip().lower()
        full_name = str(table.get("full_name") or "").strip().lower()
        if table_name:
            known_tables.add(table_name)
        if full_name:
            known_tables.add(full_name)
            known_tables.add(full_name.split(".")[-1])

        for col in table.get("columns") or []:
            col_name = str(col or "").strip().lower()
            if col_name:
                known_columns.add(col_name)

    return known_tables, known_columns


def _suggestion_has_unknown_field_refs(
    text: str,
    known_tables: set[str],
    known_columns: set[str],
) -> bool:
    suggestion = str(text or "")

    # Pattern like UF='SP' or campo = 10 should reference an existing column.
    for match in re.finditer(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\s*(?:=|!=|<>|>=|<=|>|<)\s*(?:'[^']*'|\"[^\"]*\"|\d+(?:\.\d+)?)",
        suggestion,
    ):
        field = match.group(1).lower()
        if field not in known_columns:
            return True

    # Tokens with underscore are usually schema identifiers.
    for token in re.findall(r"\b[A-Za-z][A-Za-z0-9_]*\b", suggestion):
        token_l = token.lower()
        if "_" not in token_l:
            continue
        if token_l not in known_columns and token_l not in known_tables:
            return True

    # Explicit table references like dataset.table or project.dataset.table.
    for token in re.findall(r"\b[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){1,2}\b", suggestion):
        table_tail = token.split(".")[-1].lower()
        token_l = token.lower()
        if token_l not in known_tables and table_tail not in known_tables:
            return True

    return False


def _fallback_schema_safe_suggestions(table_id: str, focal_cols: list[str]) -> list[str]:
    cols = [str(c).strip() for c in (focal_cols or []) if str(c).strip()]
    c1 = cols[0] if len(cols) > 0 else "id"
    c2 = cols[1] if len(cols) > 1 else c1
    c3 = cols[2] if len(cols) > 2 else c2

    return [
        f"Qual a distribuicao de registros por {c1} na tabela {table_id}?",
        f"Quais sao os valores mais frequentes de {c1} e {c2} em {table_id}?",
        f"Existem inconsistencias ou valores nulos em {c1}, {c2} e {c3} na tabela {table_id}?",
        f"Quais combinacoes de {c1} e {c2} concentram maior volume em {table_id}?",
        f"Como a tabela {table_id} se relaciona com outras tabelas do dataset por chaves em comum?",
    ]


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
        # Para schema_graph persiste também com chave de projeto para cache compartilhado
        if agent_id == "schema_graph" and result.get("status") == "ok":
            checkpointer.save(f"schema_graph:{project_id}", result)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/agents/query_build/suggestions")
async def query_build_suggestions(
    req: SuggestionsRequest,
    _session: dict[str, Any] = Depends(get_current_user),
):
    from src.shared.tools.bigquery import get_dataset_tables_metadata
    from langchain_core.messages import HumanMessage, SystemMessage

    project_id = req.project_id.strip()
    dataset_hint = req.dataset_hint.strip()
    table_id = req.table_id.strip()

    if not project_id or not dataset_hint or not table_id:
        raise HTTPException(status_code=400, detail="project_id, dataset_hint e table_id sao obrigatorios.")

    try:
        metadata = get_dataset_tables_metadata(project_id, dataset_hint)
        tables = metadata.get("tables", [])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar metadata: {exc}")

    # Build compact schema context
    schema_lines: list[str] = []
    focal_cols: list[str] = []
    for t in tables:
        cols = t.get("columns") or []
        schema_lines.append(f"- {t['full_name']} | colunas: {', '.join(cols) if cols else '(desconhecidas)'}")
        if t.get("table_name") == table_id or t.get("full_name", "").endswith(f".{table_id}"):
            focal_cols = cols

    schema_ctx = "\n".join(schema_lines) if schema_lines else "(schema nao disponivel)"
    focal_ctx = f"Colunas de {table_id}: {', '.join(focal_cols)}" if focal_cols else ""

    system_prompt = (
        "Voce e um analista de dados senior especialista em BigQuery. "
        "Gere exatamente 5 sugestoes de perguntas de negocio em linguagem natural "
        "que podem ser respondidas com SQL sobre o dataset informado. "
        "As sugestoes devem ser variadas: agregacoes, rankings, tendencias temporais, "
        "comparacoes e analises de relacionamento entre tabelas. "
        "REGRAS OBRIGATORIAS DE CONFIABILIDADE: "
        "(1) Use somente tabelas e colunas que existam no schema fornecido. "
        "(2) Nao invente nomes de campos, codigos, UFs, niveis, categorias ou valores literais especificos. "
        "(3) Quando um filtro depender de dominio de valores nao informado no schema, escreva de forma generica (ex: por estado de destino, por nivel de servico), sem chutar valores. "
        "(4) Se o schema nao suportar uma ideia, troque por outra pergunta viavel com as colunas reais. "
        "(5) Nao use markdown. "
        "Responda APENAS com JSON valido no formato: "
        '{\"suggestions\": [\"sugestao 1\", \"sugestao 2\", \"sugestao 3\", \"sugestao 4\", \"sugestao 5\"]}'
    )

    user_prompt = (
        f"Tabela principal em foco: {project_id}.{dataset_hint}.{table_id}\n"
        f"{focal_ctx}\n\n"
        f"Schema completo do dataset:\n{schema_ctx}\n\n"
        "Gere 5 sugestoes de perguntas de negocio que explorem bem esta tabela "
        "e seus relacionamentos com as demais tabelas do dataset. "
        "Priorize perguntas validas com o schema acima, sem citar valores especificos nao confirmados."
    )

    try:
        llm = create_llm()
        response = llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ])
        raw = response.content if hasattr(response, "content") else str(response)
        # Strip markdown fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.MULTILINE)
        raw = re.sub(r"\s*```$", "", raw.strip(), flags=re.MULTILINE)
        import json as _json
        parsed = _json.loads(raw)
        suggestions = parsed.get("suggestions", [])
        if not isinstance(suggestions, list):
            suggestions = []

        known_tables, known_columns = _extract_known_schema_tokens(tables)

        filtered: list[str] = []
        seen: set[str] = set()
        for item in suggestions:
            text = str(item or "").strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            if _suggestion_has_unknown_field_refs(text, known_tables, known_columns):
                continue
            filtered.append(text)
            seen.add(key)
            if len(filtered) == 5:
                break

        if len(filtered) < 5:
            for fb in _fallback_schema_safe_suggestions(table_id, focal_cols):
                key = fb.lower()
                if key in seen:
                    continue
                filtered.append(fb)
                seen.add(key)
                if len(filtered) == 5:
                    break

        return {"suggestions": filtered[:5]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar sugestoes: {exc}")


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


_SAFE_ID = re.compile(r"^[a-zA-Z0-9_\-]+$")


@router.get("/api/agents/schema_graph/cached/{project_id}")
async def get_cached_schema(
    project_id: str,
    _session: dict[str, Any] = Depends(get_current_user),
):
    """Retorna o último grafo de schema armazenado para um projeto, se disponível."""
    if not _SAFE_ID.match(project_id):
        raise HTTPException(status_code=400, detail="Project ID inválido.")

    checkpointer = get_checkpointer()
    cache_key = f"schema_graph:{project_id}"
    payload = checkpointer.load(cache_key)
    if payload is None:
        raise HTTPException(status_code=404, detail="Nenhum grafo em cache para este projeto.")
    return {"status": "ok", "cached": True, **payload}


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
