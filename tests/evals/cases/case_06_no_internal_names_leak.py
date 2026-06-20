"""Composer NÃO pode expor nomes de projeto/dataset/tabela no texto."""

CASE = {
    "id": "06_no_internal_names_leak",
    "question": "quantos pedidos foram processados ontem?",
    "project_id": "test_project_internal",
    "user": {"username": "tester"},
    "script": {
        "plan": {
            "rationale": "contagem simples",
            "steps": [
                {
                    "capability": "text_to_sql",
                    "args": {
                        "natural_language": "contagem de pedidos ontem",
                        "dataset_ref": "${PROJECT}.ecommerce_saude",
                    },
                }
            ],
        },
        "picker_table_ids": ["pedidos"],
        "sql": "SELECT COUNT(*) AS n FROM `test_project_internal.ecommerce_saude.pedidos`",
        "reflect": {"is_valid": True, "suggested_steps": []},
        # Resposta em linguagem de negócio (sem nome técnico).
        "composer": "Ontem foram processados **142 pedidos** no total.",
    },
    "bq": {
        "datasets": ["ecommerce_saude"],
        "tables_by_dataset": {
            "ecommerce_saude": [
                {"table_id": "pedidos",
                 "full_name": "test_project_internal.ecommerce_saude.pedidos",
                 "columns": ["id_pedido", "data_pedido"]},
            ],
        },
        "rows": [{"n": 142}],
    },
    "expect": {
        "status": "ok",
        "answer": {
            "must_mention_any": ["142", "pedidos"],
            "must_not_mention": [
                "test_project_internal",
                "ecommerce_saude",
                "ds_inteligencia_analitica",
            ],
        },
    },
}
