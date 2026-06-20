"""dataset_ref errado ("ecommerce") deve fuzzy-resolver para "ecommerce_saude"."""

CASE = {
    "id": "03_dataset_typo_fuzzy",
    "question": "quantos clientes únicos eu tenho no ecommerce?",
    "project_id": "test_project",
    "user": {"username": "tester"},
    "script": {
        "plan": {
            "rationale": "consulta direta — vai usar fuzzy fallback",
            "steps": [
                {
                    "capability": "text_to_sql",
                    "args": {
                        "natural_language": "contagem de clientes únicos",
                        # Nome errado de propósito — deve resolver fuzzy.
                        "dataset_ref": "${PROJECT}.ecommerce",
                    },
                }
            ],
        },
        "picker_table_ids": ["clientes"],
        "sql": (
            "SELECT COUNT(DISTINCT id_cliente) AS clientes_unicos "
            "FROM `test_project.ecommerce_saude.clientes`"
        ),
        "reflect": {"is_valid": True, "suggested_steps": []},
        "composer": "Você tem **3.421 clientes únicos** cadastrados na base.",
    },
    "bq": {
        # 'ecommerce' não existe; só 'ecommerce_saude' existe → fuzzy resolve.
        "datasets": ["ds_inteligencia_analitica", "ecommerce_saude",
                     "logistica_vendas"],
        "tables_by_dataset": {
            "ecommerce_saude": [
                {"table_id": "clientes",
                 "full_name": "test_project.ecommerce_saude.clientes",
                 "columns": ["id_cliente", "nome_completo"]},
            ],
        },
        "rows": [{"clientes_unicos": 3421}],
    },
    "expect": {
        "status": "ok",
        "plan": {"must_include": ["text_to_sql"]},
        "steps": {"text_to_sql": {"ok": True}},
        "answer": {
            "must_mention_any": ["3.421", "3421", "clientes"],
        },
    },
}
