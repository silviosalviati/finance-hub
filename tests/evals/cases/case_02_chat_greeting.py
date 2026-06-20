"""Saudações puramente conversacionais devem ir para chat_answer."""

CASE = {
    "id": "02_chat_greeting",
    "question": "oi, tudo bem? você é o Finance Voice IA?",
    "project_id": "test_project",
    "user": {"username": "tester"},
    "user_profile": {},
    "script": {
        "plan": {
            "rationale": "saudação — sem dados envolvidos",
            "steps": [
                {"capability": "chat_answer", "args": {}, "rationale": "social"}
            ],
        },
        "reflect": {"is_valid": True, "suggested_steps": []},
        "composer": "Tudo certo! Sou o Finance Voice IA. Posso te ajudar a analisar seus dados — é só perguntar.",
    },
    "bq": {},
    "expect": {
        "status": "ok",
        "plan": {
            "must_include": ["chat_answer"],
            "must_not_include": ["text_to_sql", "bq_query"],
            "max_steps": 1,
        },
        "answer": {
            "must_mention_any": ["finance voice", "ajudar", "analisar"],
            "min_length": 10,
        },
    },
}
