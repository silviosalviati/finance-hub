from __future__ import annotations

from src.api.routes.agents import (
    _extract_user_name,
    _find_repeated_response,
    _is_analytics_query,
    _is_asking_name,
    _is_confirmation_reply,
    _normalize_text,
    _resolve_pending_confirmation,
)


def test_extract_user_name_from_phrase():
    assert _extract_user_name("Oi, meu nome é Jao") == "Jao"
    assert _extract_user_name("Eu me chamo Maria Clara") == "Maria Clara"


def test_detect_ask_name_intent():
    assert _is_asking_name("qual meu nome?") is True
    assert _is_asking_name("voce lembra meu nome") is True
    assert _is_asking_name("analise o mes passado") is False


def test_analytics_query_classifier():
    # Termos genéricos de análise de dados (sem domínio fixo).
    assert _is_analytics_query("Quero um relatório dos últimos 30 dias") is True
    assert _is_analytics_query("Mostre uma tabela com o total agrupado") is True
    assert _is_analytics_query("Gere um gráfico de tendência") is True
    assert _is_analytics_query(
        "quero saber quais sao os maiores clientes que realizam pagamento via pix no meu ecommerce saude"
    ) is True
    assert _is_analytics_query("contas a pagar em atraso") is True
    assert _is_analytics_query("qual meu nome") is False
    assert _is_analytics_query("oi tudo bem?") is False


def test_repeated_response_reuses_previous_payload():
    previous = {
        "status": "ok",
        "response_mode": "chat",
        "chat_answer": "Seu nome é Jao.",
    }
    turns = [
        {
            "query_norm": _normalize_text("qual meu nome"),
            "response": previous,
        }
    ]

    reused = _find_repeated_response(turns, _normalize_text("Qual meu nome"))

    assert reused is not None
    assert reused["response_mode"] == "chat"
    assert reused.get("response_reused") is True
    assert "Pergunta repetida" in " ".join(reused.get("warnings", []))


def test_is_confirmation_reply_detecta_palavras_curtas():
    assert _is_confirmation_reply("sim") is True
    assert _is_confirmation_reply("Sim!") is True
    assert _is_confirmation_reply("pode") is True
    assert _is_confirmation_reply("ok, manda") is True
    assert _is_confirmation_reply("não") is False
    # Mensagem longa com conteúdo próprio não é só uma confirmação.
    assert _is_confirmation_reply(
        "sim, mas quero focar só nos clientes inadimplentes deste mês"
    ) is False


def test_resolve_pending_confirmation_religa_a_pergunta_anterior():
    turns = [
        {
            "query": "Compare os KPIs oficiais deste mês versus o mês anterior.",
            "answer_text": "Os KPIs oficiais são X, Y, Z. Posso prosseguir com o cálculo?",
        }
    ]

    resolved = _resolve_pending_confirmation("sim", turns)

    assert "Compare os KPIs oficiais" in resolved
    assert "sim" in resolved
    # Sem pergunta pendente (resposta anterior não termina em "?"): não reescreve.
    turns_sem_pergunta = [{"query": "oi", "answer_text": "Olá! Como posso ajudar?"[:-1]}]
    assert _resolve_pending_confirmation("sim", turns_sem_pergunta) == "sim"
    # Sem turnos anteriores: não reescreve.
    assert _resolve_pending_confirmation("sim", []) == "sim"
    # Não é uma confirmação (pergunta nova de verdade): não reescreve.
    assert _resolve_pending_confirmation("quanto vendemos hoje?", turns) == "quanto vendemos hoje?"
