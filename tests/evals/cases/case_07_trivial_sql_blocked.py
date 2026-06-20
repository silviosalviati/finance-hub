"""SQL placeholder (sem FROM real) deve ser rejeitado pelo guard."""

CASE = {
    "id": "07_trivial_sql_blocked",
    "question": "qual o churn de clientes esse mês?",
    "project_id": "test_project",
    "user": {"username": "tester"},
    "script": {
        "plan": {
            "rationale": "consulta de churn",
            "steps": [
                {
                    "capability": "text_to_sql",
                    "args": {
                        "natural_language": "churn de clientes do mês",
                        "dataset_ref": "${PROJECT}.ecommerce_saude",
                    },
                }
            ],
        },
        "picker_table_ids": ["clientes"],
        # SQL trivial: o LLM "desistiu" e devolveu mensagem como string.
        "sql": "SELECT 'nao foi possivel calcular churn' AS erro",
        "reflect": {"is_valid": True, "suggested_steps": []},
        "composer": (
            "Não consegui calcular o churn com os dados disponíveis até "
            "agora. Posso reformular a query — tenta agora?"
        ),
    },
    "bq": {
        "datasets": ["ecommerce_saude"],
        "tables_by_dataset": {
            "ecommerce_saude": [
                {"table_id": "clientes",
                 "full_name": "test_project.ecommerce_saude.clientes",
                 "columns": ["id_cliente", "data_cadastro"]},
            ],
        },
        "rows": [],
    },
    "expect": {
        # O step `text_to_sql` deve falhar (guard de SQL trivial).
        "steps": {"text_to_sql": {"ok": False}},
        "answer": {
            "must_not_mention": [
                "nao foi possivel calcular churn AS erro",
                "SELECT 'nao foi possivel",
            ],
        },
    },
}
