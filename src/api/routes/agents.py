from __future__ import annotations

import asyncio
import json
import logging
import re
import traceback
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from src.agents.finance_auditor.capabilities import resolve_dataset_by_gerencia
from src.shared.guardrails import rbac as finance_rbac
from src.api.dependencies import get_checkpointer, get_current_user, get_registry
from src.shared.config import get_runtime_config
from src.shared.tools.bigquery import (
    get_dataset_tables_metadata,
    get_dataset_tables_schema,
    validate_dataset_for_query_build,
    validate_query_context_for_query_analyzer,
)
from src.shared.tools.llm import create_llm, invoke_with_retry, invoke_with_retry_async
from src.shared.tools.schemas import SuggestionsResponse

router = APIRouter(tags=["agents"])


class AnalyzeRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=32_000)
    project_id: str | None = None
    dataset_hint: str | None = None
    thread_id: str | None = None
    # Fase 4: anexos opcionais (CSV/imagem em base64). Cada item:
    # {"kind": "csv"|"image", "data": "<base64>", "filename": "<opcional>"}
    attachments: list[dict[str, Any]] | None = Field(default=None, max_length=5)


class ResumeAnalyzerRequest(BaseModel):
    thread_id: str = Field(..., min_length=1, max_length=128)
    decision: str = Field(..., min_length=1, max_length=2048)  # "approve" | "skip"


class ValidateDatasetRequest(BaseModel):
    project_id: str = Field(..., min_length=1, max_length=256)
    dataset_hint: str = Field(..., min_length=1, max_length=256)


class ResolveGerenciaRequest(BaseModel):
    gerencia: str = Field(..., min_length=1, max_length=200)
    project_id: str | None = None


class ValidateAnalyzerContextRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=32_000)
    project_id: str | None = None


class SuggestionsRequest(BaseModel):
    project_id: str = Field(..., min_length=1, max_length=256)
    dataset_hint: str = Field(..., min_length=1, max_length=256)
    table_id: str = Field(..., min_length=1, max_length=256)


class GerenciaRequest(BaseModel):
    gerencia: str = Field(..., min_length=1, max_length=200)
    project_id: str | None = None
    label: str | None = Field(default=None, max_length=200)


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


from collections import deque
from functools import lru_cache as _lru_cache
from threading import Lock as _RLock

_QA_RATE_LIMIT_WINDOW = 60.0
_QA_RATE_LIMIT_MAX = int(get_runtime_config("QA_RATE_LIMIT_PER_MIN", "10"))
_QA_RATE_BUCKETS: dict[str, deque] = {}
_QA_RATE_LOCK = _RLock()


def _qa_rate_limit_check(token: str) -> None:
    """Sliding-window rate limiter per user token. Raises HTTPException on excess."""
    import time as _time
    now = _time.time()
    with _QA_RATE_LOCK:
        bucket = _QA_RATE_BUCKETS.setdefault(token, deque())
        while bucket and now - bucket[0] > _QA_RATE_LIMIT_WINDOW:
            bucket.popleft()
        if len(bucket) >= _QA_RATE_LIMIT_MAX:
            retry = int(_QA_RATE_LIMIT_WINDOW - (now - bucket[0]))
            raise HTTPException(
                status_code=429,
                detail=f"Muitas análises seguidas. Tente novamente em {max(retry, 1)}s.",
            )
        bucket.append(now)
        # opportunistic cleanup: drop empty buckets so dict doesn't grow forever
        if len(_QA_RATE_BUCKETS) > 1024:
            stale = [k for k, dq in _QA_RATE_BUCKETS.items() if not dq]
            for k in stale:
                _QA_RATE_BUCKETS.pop(k, None)


_NAME_PATTERNS = (
    re.compile(r"\bmeu nome\s*(?:é|e)\s*([a-zA-ZÀ-ÿ][\wÀ-ÿ\- ]{0,40})", re.IGNORECASE),
    re.compile(r"\bme chamo\s+([a-zA-ZÀ-ÿ][\wÀ-ÿ\- ]{0,40})", re.IGNORECASE),
)
_ASK_NAME_PATTERN = re.compile(
    r"\b(qual(?:\s*(?:é|e))?\s*meu\s*nome|lembra\s*meu\s*nome|sabe\s*meu\s*nome)\b",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-ZÀ-ÿ0-9_]+", text.lower())
        if len(token) > 2
    }


def _is_analytics_query(query: str) -> bool:
    """Heurística leve: decide se a pergunta provavelmente envolve dados.

    Sem termos de domínio (VoC/fricção/sentimento foram removidos). A decisão
    final é do Planner do Supervisor; este filtro serve apenas para roteamento
    rápido entre o caminho conversacional (RAG curto) e o caminho do grafo
    completo, mantendo o comportamento do chat para perguntas sociais.
    """
    if _is_asking_name(query):
        return False

    q = _normalize_text(query)
    analytics_terms = (
        "analise", "análise", "relatorio", "relatório",
        "dado", "dados", "tabela", "tabelas", "dataset", "datasets",
        "query", "sql", "consulta", "consultas",
        "grafico", "gráfico", "graficos", "gráficos",
        "estatistica", "estatística", "media", "média", "mediana",
        "soma", "total", "contagem", "agrupado", "agrupar",
        "periodo", "período", "ultimos", "últimos", "mes", "mês",
        "compare", "comparar", "tendencia", "tendência",
        "quanto", "quantos", "qual", "quais", "maior", "maiores",
        "menor", "menores", "ranking", "top", "cliente", "clientes",
        "pedido", "pedidos", "pagamento", "pagamentos", "pix",
        "venda", "vendas", "receita", "faturamento", "inadimplencia",
        "inadimplência", "cobranca", "cobrança", "contas a pagar",
        "contas a receber", "ecommerce", "e-commerce",
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


_CONFIRMATION_WORDS = {
    "sim", "ok", "okay", "claro", "certo", "certeza",
    "pode", "podes", "vai", "manda", "confirmo", "confirmado",
    "afirmativo", "isso", "exato", "correto", "prossiga", "prosseguir",
    "continua", "continue", "vamos", "perfeito", "positivo",
}


def _is_confirmation_reply(query: str) -> bool:
    """Resposta curta de confirmação (sim/ok/pode/...), sem conteúdo
    analítico próprio — só faz sentido lida junto da pergunta anterior."""
    normalized = _normalize_text(query).strip(" .,!?;:")
    if not normalized:
        return False
    words = normalized.split()
    if not words or len(words) > 4:
        return False
    return any(w in _CONFIRMATION_WORDS for w in words)


def _resolve_pending_confirmation(query: str, turns: list[dict[str, Any]]) -> str:
    """Religa uma confirmação curta ("sim") à pergunta original quando a
    última resposta do bot terminou em pergunta (ex.: "Posso prosseguir?").

    Sem isso, "sim" chega ao roteador como uma pergunta nova e sem contexto
    próprio: não tem termo analítico (cai no chat genérico) e não tem
    nenhuma palavra em comum com o turno anterior (a busca por relevância
    léxica em `_retrieve_relevant_turns` não encontra nada) — o agente
    responde algo genérico, ignorando a confirmação do usuário por completo.
    """
    if not turns or not _is_confirmation_reply(query):
        return query

    last_answer = str(turns[-1].get("answer_text") or "").strip()
    if not last_answer.endswith("?"):
        return query

    pending_query = str(turns[-1].get("query") or "").strip()
    if not pending_query:
        return query

    return (
        f"{pending_query} (o usuário confirmou com \"{query.strip()}\" — "
        "prossiga agora com a análise completa, sem perguntar de novo)"
    )


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


@_lru_cache(maxsize=1)
def _get_shared_llm() -> BaseChatModel:
    return create_llm()


def _llm_text(response: Any) -> str:
    if hasattr(response, "content"):
        return str(response.content)
    return str(response)


async def _build_rag_chat_answer(query: str, profile: dict[str, Any], relevant_turns: list[dict[str, Any]]) -> str:
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
        llm = _get_shared_llm()
        response = await invoke_with_retry_async(
            llm,
            [
                SystemMessage(
                    content=(
                        "Você é o Finance Voice IA em modo conversacional. "
                        "Responda em português, de forma objetiva e útil. "
                        "Quando houver memória de sessão, use-a para responder."
                    )
                ),
                HumanMessage(
                    content=(
                        f"Pergunta atual do usuário: {query}\n\n"
                        "Contexto recuperado (RAG lexical da sessão):\n"
                        f"{chr(10).join(context_lines)}\n\n"
                        "Responda em até 5 linhas."
                    )
                ),
            ],
            label="chat_fallback",
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
        "Posso te ajudar com perguntas financeiras ou da sessão atual. "
        "Para uma análise completa, descreva o período ou tema de interesse."
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

    if agent_id == "query_analyzer":
        _qa_rate_limit_check(session["token"])
    if agent_id == "finance_auditor" and not project_id:
        project_id = get_runtime_config("FINANCE_AUDITOR_DEFAULT_PROJECT", "silviosalviati")

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

            # "sim"/"pode"/"ok" em resposta a uma pergunta do bot ("Posso
            # prosseguir?") religa à pergunta original — ver docstring.
            # Usado daqui pra baixo no lugar de `query` cru; o histórico
            # (turns) continua guardando o que o usuário de fato digitou.
            effective_query = _resolve_pending_confirmation(query, turns)

            remembered_name = _extract_user_name(effective_query)
            if remembered_name:
                profile["name"] = remembered_name
                response = _build_finance_chat_response(
                    f"Perfeito, vou lembrar. Seu nome é {remembered_name}."
                )
            elif _is_asking_name(effective_query):
                name = str(profile.get("name") or "").strip()
                if name:
                    response = _build_finance_chat_response(f"Seu nome é {name}.")
                else:
                    response = _build_finance_chat_response(
                        "Ainda não tenho seu nome salvo nesta sessão. "
                        "Pode me dizer algo como: 'meu nome é João'."
                    )
            elif not _is_analytics_query(effective_query):
                relevant_turns = _retrieve_relevant_turns(turns, effective_query)
                answer = await _build_rag_chat_answer(effective_query, profile, relevant_turns)
                response = _build_finance_chat_response(answer)
            else:
                # agent.analyze é síncrono e pode levar muitos segundos
                # (LLM + BigQuery) — roda numa thread para não travar o
                # event loop e, com ele, todo usuário concorrente.
                response = await asyncio.to_thread(
                    agent.analyze,
                    query=effective_query,
                    project_id=project_id,
                    dataset_hint=req.dataset_hint or profile.get("pinned_dataset_ref"),
                    user_profile=profile,
                    user=session,
                    attachments=req.attachments or [],
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

        analyze_kwargs: dict = {"query": query, "project_id": project_id, "dataset_hint": req.dataset_hint}
        if agent_id == "query_analyzer" and req.thread_id:
            analyze_kwargs["thread_id"] = req.thread_id
        if agent_id == "query_build":
            analyze_kwargs["user"] = session
            if req.thread_id:
                analyze_kwargs["thread_id"] = req.thread_id
        # Mesmo motivo do finance_auditor acima: síncrono e potencialmente
        # lento (LLM/BigQuery), roda fora do event loop.
        result = await asyncio.to_thread(agent.analyze, **analyze_kwargs)
        checkpoint_key = f"{session['token']}-{agent_id}"
        checkpointer.save(checkpoint_key, result)
        # Para schema_graph persiste também com chave de projeto para cache compartilhado
        if agent_id == "schema_graph" and result.get("status") == "ok":
            checkpointer.save(f"schema_graph:{project_id}", result)
        return result
    except HTTPException:
        raise
    except Exception as exc:
        logging.error("Erro no analyze de %s: %s\n%s", agent_id, exc, traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail="Erro interno ao processar a análise. Tente novamente em instantes.",
        )


@router.post("/api/agents/query_build/suggestions")
async def query_build_suggestions(
    req: SuggestionsRequest,
    _session: dict[str, Any] = Depends(get_current_user),
):
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
        llm = _get_shared_llm()
        structured_llm = llm.with_structured_output(SuggestionsResponse)
        result: SuggestionsResponse = await invoke_with_retry_async(
            structured_llm,
            [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ],
            label="table_suggestions",
        )
        suggestions = result.suggestions if result else []

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


# ── Gerência → dataset (rótulos BigQuery) ────────────────────────────────────

_GERENCIA_CATALOG_CACHE_TTL = 600.0
_gerencia_catalog_cache: dict[str, tuple[dict[str, Any], float]] = {}


def _get_cached_dataset_catalog(project_id: str, dataset_id: str) -> dict[str, Any]:
    import time as _time

    dataset_ref = f"{project_id}.{dataset_id}"
    cached = _gerencia_catalog_cache.get(dataset_ref)
    now = _time.monotonic()
    if cached and now - cached[1] < _GERENCIA_CATALOG_CACHE_TTL:
        return cached[0]
    info = get_dataset_tables_schema(project_id, dataset_id, max_tables=50, max_columns=50)
    _gerencia_catalog_cache[dataset_ref] = (info, now)
    return info


_GERENCIA_SUGGESTIONS_SYSTEM_PROMPT = (
    "Voce e um analista de dados senior. Gere exatamente 6 perguntas de negocio "
    "em linguagem natural, em portugues, sobre os dados disponiveis nesta area — "
    "duas para cada papel abaixo, NESTA ORDEM EXATA (as duas perguntas de um "
    "mesmo papel devem explorar angulos diferentes, sem se repetir): "
    "1-2. Diretor: visao estrategica — impacto no resultado, metas, risco do negocio. "
    "3-4. Gerente: visao tatica — comparacoes entre periodos, segmentacoes, tendencias. "
    "5-6. Coordenador: visao operacional — o que fazer, prazos, prioridades, casos especificos. "
    "CADA PERGUNTA PRECISA SER ESPECIFICA A ESTA AREA, NUNCA GENERICA: cite, em "
    "linguagem de negocio (nunca o nome tecnico da tabela/coluna), um conceito ou "
    "relacao que SO EXISTE neste schema — um tipo de registro, uma relacao entre "
    "duas tabelas, uma dimensao de tempo/categoria que de fato aparece nas colunas "
    "fornecidas. Uma pergunta que serviria pra qualquer area do negocio (financeira, "
    "vendas, RH...) sem mudar uma palavra esta errada — refaca ate amarrar num "
    "elemento real deste schema. "
    "Errado (generico, serve pra qualquer area): \"Qual o impacto dos principais "
    "indicadores no resultado do periodo?\" "
    "Certo (especifico — exemplo pra um schema de cobranca com tabelas de "
    "parcelas e clientes): \"Quais faixas de atraso da carteira concentram mais "
    "valor em aberto?\" "
    "REGRAS OBRIGATORIAS DE CONFIABILIDADE: "
    "(1) Use somente tabelas e colunas que existam no schema fornecido. "
    "(2) Nao invente nomes de campos, codigos, categorias ou valores literais especificos. "
    "(3) Nao cite nomes tecnicos de tabelas, colunas ou SQL na pergunta — escreva em "
    "linguagem de negocio. "
    "(4) Nao use markdown. "
    "Responda APENAS com JSON valido no formato: "
    '{\"suggestions\": [\"pergunta 1 do diretor\", \"pergunta 2 do diretor\", '
    '\"pergunta 1 do gerente\", \"pergunta 2 do gerente\", '
    '\"pergunta 1 do coordenador\", \"pergunta 2 do coordenador\"]}'
)


def _fallback_gerencia_suggestions(tables: list[dict[str, Any]]) -> list[str]:
    if not tables:
        return [
            "Qual o impacto dos principais indicadores desta area no resultado do periodo?",
            "Esses indicadores sustentam a meta do periodo para esta area?",
            "Como os principais indicadores desta area se comparam ao periodo anterior?",
            "Quero abrir esses indicadores por segmento ou unidade.",
            "Quais casos desta area precisam de acao prioritaria agora?",
            "Quais sao os principais indicadores disponiveis nesta area?",
        ]
    def _table_and_cols(idx: int) -> tuple[str, str, str]:
        table = tables[idx % len(tables)]
        cols = [c.get("name") for c in (table.get("columns") or []) if c.get("name")]
        label = table.get("table_id") or "esta base"
        c1 = cols[0] if len(cols) > 0 else "id"
        c2 = cols[1] if len(cols) > 1 else c1
        return label, c1, c2

    # Usa uma tabela diferente por papel quando houver mais de uma — sem
    # isso as 6 perguntas deste fallback (só dispara se a LLM falhar) saem
    # todas amarradas na mesma tabela, parecendo ainda mais repetitivas.
    t0, _, _ = _table_and_cols(0)
    t1, c1_1, c1_2 = _table_and_cols(1)
    t2, _, _ = _table_and_cols(2)
    return [
        f"Qual o impacto de {t0} no resultado geral do periodo?",
        f"Existe algum risco relevante refletido em {t0}?",
        f"Como {c1_1} se compara entre os periodos mais recentes em {t1}?",
        f"Como {c1_1} se relaciona com {c1_2} em {t1}?",
        f"Quais casos em {t2} precisam de acao prioritaria agora?",
        f"Quais registros de {t2} preciso revisar hoje?",
    ]


async def _generate_gerencia_suggestions(tables: list[dict[str, Any]]) -> list[str]:
    schema_lines = [
        f"- {t.get('full_name', '')} | colunas: "
        f"{', '.join(c.get('name', '') for c in (t.get('columns') or []))}"
        for t in tables
    ]
    schema_ctx = "\n".join(schema_lines) if schema_lines else "(schema nao disponivel)"
    user_prompt = (
        f"Tabelas disponiveis nesta area:\n{schema_ctx}\n\n"
        "Gere 6 perguntas de negocio variadas que explorem bem esses dados."
    )
    known_tables, known_columns = _extract_known_schema_tokens(tables)

    suggestions: list[str] = []
    try:
        llm = _get_shared_llm()
        structured_llm = llm.with_structured_output(SuggestionsResponse)
        result: SuggestionsResponse = await invoke_with_retry_async(
            structured_llm,
            [
                SystemMessage(content=_GERENCIA_SUGGESTIONS_SYSTEM_PROMPT),
                HumanMessage(content=user_prompt),
            ],
            label="gerencia_suggestions",
        )
        suggestions = result.suggestions if result else []
    except Exception:
        suggestions = []

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
        if len(filtered) == 6:
            break

    if len(filtered) < 6:
        for fb in _fallback_gerencia_suggestions(tables):
            key = fb.lower()
            if key in seen:
                continue
            filtered.append(fb)
            seen.add(key)
            if len(filtered) == 6:
                break

    return filtered[:6]


def _sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


async def _gerencia_stream(req: GerenciaRequest, session: dict[str, Any]):
    """Resolve a área (gerência) escolhida pelo usuário para um dataset real via
    rótulo do BigQuery, "aprende" seu catálogo (tabelas/colunas/tipos/descrições)
    e fixa o resultado na sessão de chat para as próximas perguntas.

    Emite um evento `phase` ao concluir cada etapa (não antes de começar —
    a UI já mostra a fase seguinte enquanto ela roda) para o frontend
    refletir o progresso real em vez de advinhar com um timer.
    """
    gerencia = req.gerencia.strip()
    project_id = (req.project_id or "").strip() or get_runtime_config(
        "FINANCE_AUDITOR_DEFAULT_PROJECT", "silviosalviati"
    )

    match = resolve_dataset_by_gerencia(project_id, gerencia)
    if not match:
        yield _sse({
            "status": "not_found",
            "gerencia": gerencia,
            "message": "Ainda não encontrei uma base de dados rotulada para esta área.",
        })
        return

    dataset_id = match["dataset_id"]
    allowed, reason = finance_rbac.check_dataset(session, dataset_id)
    if not allowed:
        yield _sse({
            "status": "denied",
            "gerencia": gerencia,
            "message": f"Acesso negado a esta área: {reason}",
        })
        return

    yield _sse({"phase": "catalog"})
    try:
        catalog = _get_cached_dataset_catalog(project_id, dataset_id)
    except Exception as exc:
        yield _sse({
            "status": "error",
            "gerencia": gerencia,
            "message": f"Falha ao carregar o catálogo desta área: {exc}",
        })
        return

    tables = catalog.get("tables") or []
    dataset_ref = catalog.get("dataset_ref") or f"{project_id}.{dataset_id}"

    yield _sse({"phase": "suggestions"})
    suggestions = await _generate_gerencia_suggestions(tables)

    checkpointer = get_checkpointer()
    chat_session = _load_finance_chat_session(session["token"], checkpointer)
    profile: dict[str, Any] = chat_session.get("profile", {})
    profile["pinned_dataset_ref"] = dataset_ref
    profile["pinned_gerencia"] = match["gerencia"]
    chat_session["profile"] = profile
    chat_session["updated_at"] = _now_iso()
    _save_finance_chat_session(session["token"], checkpointer, chat_session)

    yield _sse({
        "status": "ok",
        "gerencia": gerencia,
        "dataset_ref": dataset_ref,
        "table_count": len(tables),
        "message": f"Estou pronto para responder perguntas sobre {(req.label or '').strip() or match['gerencia']}.",
        "suggestions": suggestions,
    })


@router.post("/api/agents/finance_auditor/gerencia")
async def select_finance_gerencia(
    req: GerenciaRequest,
    session: dict[str, Any] = Depends(get_current_user),
):
    return StreamingResponse(_gerencia_stream(req, session), media_type="text/event-stream")


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


@router.post("/api/agents/query_build/resolve-gerencia")
async def resolve_query_build_gerencia(
    req: ResolveGerenciaRequest,
    session: dict[str, Any] = Depends(get_current_user),
):
    """Resolve o dataset da gerência do usuário pra abrir o Query Builder
    sem precisar passar pelo Schema Explorer. RBAC aqui é só pra não mostrar
    um badge "pronto" enganoso — `check_access` no grafo continua sendo a
    autoridade real no momento de gerar a SQL.
    """
    project_id = (req.project_id or "").strip() or get_runtime_config(
        "FINANCE_AUDITOR_DEFAULT_PROJECT", "silviosalviati"
    )
    match = resolve_dataset_by_gerencia(project_id, req.gerencia.strip())
    if not match:
        return {"valid": False, "message": "Não encontramos uma base de dados para esta gerência."}

    allowed, reason = finance_rbac.check_dataset(session, match["dataset_id"])
    if not allowed:
        return {"valid": False, "message": f"Acesso negado a esta área: {reason}"}

    return {
        "valid": True,
        "project_id": project_id,
        "dataset_id": match["dataset_id"],
        "gerencia": match["gerencia"],
    }


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


@router.post("/api/agents/query_analyzer/resume")
async def resume_query_analyzer(
    req: ResumeAnalyzerRequest,
    session: dict[str, Any] = Depends(get_current_user),
):
    """Retoma o pipeline do Query Analyzer após decisão humana.

    Envie `decision: "approve"` para prosseguir com a otimização ou
    `decision: "skip"` para ir direto ao relatório sem otimizar.
    """
    registry = get_registry()
    try:
        agent = registry.get("query_analyzer")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        result = agent.resume(thread_id=req.thread_id, human_decision=req.decision)
        checkpointer = get_checkpointer()
        checkpointer.save(f"{session['token']}-query_analyzer", result)
        return result
    except RuntimeError as exc:
        # erros de fluxo previsíveis (sessão expirou, sem relatório) — mensagem direta
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        logging.error("Erro no resume do query_analyzer: %s\n%s", exc, traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail="Erro interno ao retomar a análise. Tente novamente em instantes.",
        )


@router.post("/api/agents/query_build/resume")
async def resume_query_build(
    req: ResumeAnalyzerRequest,
    session: dict[str, Any] = Depends(get_current_user),
):
    """Retoma o pipeline do Query Builder após decisão humana sobre o score de qualidade.

    Envie `decision: "seguir"` para aceitar a SQL como está, ou
    `decision: "melhorar"` para voltar ao nó de construção com o score e os
    problemas identificados como contexto de correção.
    """
    registry = get_registry()
    try:
        agent = registry.get("query_build")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        result = agent.resume(thread_id=req.thread_id, human_decision=req.decision)
        checkpointer = get_checkpointer()
        checkpointer.save(f"{session['token']}-query_build", result)
        return result
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        logging.error("Erro no resume do query_build: %s\n%s", exc, traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail="Erro interno ao retomar a geração de SQL. Tente novamente em instantes.",
        )


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
