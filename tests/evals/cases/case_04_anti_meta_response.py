"""Composer NÃO pode usar frases proibidas mesmo quando algo falhou."""

CASE = {
    "id": "04_anti_meta_response",
    "question": "qual foi a venda média no último mês?",
    "project_id": "test_project",
    "user": {"username": "tester"},
    "script": {
        "plan": {
            "rationale": "consulta de venda média",
            "steps": [
                {
                    "capability": "text_to_sql",
                    "args": {
                        "natural_language": "venda média no último mês",
                        "dataset_ref": "${PROJECT}.ecommerce_saude",
                    },
                }
            ],
        },
        "picker_table_ids": ["pedidos"],
        "sql": "SELECT AVG(valor_total) AS ticket_medio FROM `test_project.ecommerce_saude.pedidos`",
        "reflect": {"is_valid": True, "suggested_steps": []},
        # Composer responde bem — sem frases proibidas, sem nomes internos.
        "composer": (
            "O ticket médio dos pedidos no período foi **R$ 187,40**. "
            "Sigo à disposição para explorar a quebra por categoria ou canal."
        ),
    },
    "bq": {
        "datasets": ["ecommerce_saude"],
        "tables_by_dataset": {
            "ecommerce_saude": [
                {"table_id": "pedidos",
                 "full_name": "test_project.ecommerce_saude.pedidos",
                 "columns": ["id_pedido", "valor_total", "data_pedido"]},
            ],
        },
        "rows": [{"ticket_medio": 187.40}],
    },
    "expect": {
        "status": "ok",
        "answer": {
            "must_mention_any": ["187", "ticket", "médio"],
            "must_not_mention": [
                "tente refazer", "tente novamente",
                "limitação interna", "problema técnico",
                "verifique no bigquery", "revise a estrutura",
                "consulta que seria executada",
            ],
        },
    },
}
