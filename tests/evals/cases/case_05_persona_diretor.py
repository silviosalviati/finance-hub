"""Persona Diretor é detectada e fluida pelo grafo até o final."""

CASE = {
    "id": "05_persona_diretor",
    "question": "preciso de uma visão executiva sobre o impacto financeiro do PIX para a diretoria",
    "project_id": "test_project",
    "user": {"username": "ceo", "is_admin": False},
    "user_profile": {},
    "script": {
        "plan": {
            "rationale": "tema estratégico",
            "steps": [
                {
                    "capability": "text_to_sql",
                    "args": {
                        "natural_language": "impacto financeiro do pix",
                        "dataset_ref": "${PROJECT}.ecommerce_saude",
                    },
                }
            ],
        },
        "picker_table_ids": ["pagamentos"],
        "sql": "SELECT SUM(valor_pago) AS total_pix FROM `test_project.ecommerce_saude.pagamentos` WHERE metodo_pagamento='Pix'",
        "reflect": {"is_valid": True, "suggested_steps": []},
        "composer": (
            "## Resumo executivo\n\n"
            "O Pix concentra **R$ 1,2 mi** da receita do trimestre — "
            "cerca de 38% do total transacionado."
        ),
    },
    "bq": {
        "datasets": ["ecommerce_saude"],
        "tables_by_dataset": {
            "ecommerce_saude": [
                {"table_id": "pagamentos",
                 "full_name": "test_project.ecommerce_saude.pagamentos",
                 "columns": ["metodo_pagamento", "valor_pago"]},
            ],
        },
        "rows": [{"total_pix": 1200000.0}],
    },
    "expect": {
        "status": "ok",
        "persona": "diretor",
        "plan": {"must_include": ["text_to_sql"]},
    },
}
