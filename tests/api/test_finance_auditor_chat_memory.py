from __future__ import annotations

from src.api.routes.agents import (
    _extract_user_name,
    _find_repeated_response,
    _is_analytics_query,
    _is_asking_name,
    _normalize_text,
)


def test_extract_user_name_from_phrase():
    assert _extract_user_name("Oi, meu nome é Jao") == "Jao"
    assert _extract_user_name("Eu me chamo Maria Clara") == "Maria Clara"


def test_detect_ask_name_intent():
    assert _is_asking_name("qual meu nome?") is True
    assert _is_asking_name("voce lembra meu nome") is True
    assert _is_asking_name("analise o mes passado") is False


def test_analytics_query_classifier():
    assert _is_analytics_query("Quero um relatório de fricção") is True
    assert _is_analytics_query("Analise os atendimentos do mês passado") is True
    assert _is_analytics_query("qual meu nome") is False


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
