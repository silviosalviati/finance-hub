"""Pergunta analítica sobre cliente/pagamento DEVE virar plano com text_to_sql.

Regressão para o bug em que o Planner escolhia chat_answer e respondia
generalidades sobre "Geração Z" em vez de consultar dados.
"""

CASE = {
    "id": "01_analytical_pix",
    "question": "qual é o cliente que mais realiza pagamento via Pix no meu ecommerce de saúde?",
    "project_id": "test_project",
    "user": {"username": "tester"},
    "user_profile": {},
    "script": {
        "plan": {
            "rationale": "pergunta analítica sobre clientes/pagamentos",
            "steps": [
                {
                    "capability": "text_to_sql",
                    "args": {
                        "natural_language": "maior cliente por valor pago em pix",
                        "dataset_ref": "${PROJECT}.ecommerce_saude",
                        "row_limit": 10,
                    },
                    "rationale": "responder direto via SQL",
                }
            ],
        },
        "picker_table_ids": ["clientes", "pedidos", "pagamentos"],
        "sql": (
            "SELECT c.nome_completo, SUM(p.valor_pago) AS total_pix "
            "FROM `test_project.ecommerce_saude.pagamentos` p "
            "JOIN `test_project.ecommerce_saude.pedidos` pe "
            "  ON p.id_pedido = pe.id_pedido "
            "JOIN `test_project.ecommerce_saude.clientes` c "
            "  ON pe.id_cliente = c.id_cliente "
            "WHERE p.metodo_pagamento = 'Pix' "
            "GROUP BY c.nome_completo "
            "ORDER BY total_pix DESC LIMIT 10"
        ),
        "reflect": {"is_valid": True, "suggested_steps": []},
        "composer": (
            "Os cinco maiores clientes em pagamentos por Pix são "
            "**Ana Souza** (R$ 12.450), Bruno Lima, Carla Mendes, Diogo "
            "Pires e Eva Rocha. Ana lidera com folga, respondendo por "
            "cerca de 18% do total recebido por Pix no período."
        ),
    },
    "bq": {
        "datasets": ["ecommerce_saude", "ds_inteligencia_analitica",
                     "logistica_vendas", "inteligencia_negocios"],
        "tables_by_dataset": {
            "ecommerce_saude": [
                {"table_id": "clientes",
                 "full_name": "test_project.ecommerce_saude.clientes",
                 "columns": ["id_cliente", "nome_completo", "email"]},
                {"table_id": "pedidos",
                 "full_name": "test_project.ecommerce_saude.pedidos",
                 "columns": ["id_pedido", "id_cliente", "valor_total"]},
                {"table_id": "pagamentos",
                 "full_name": "test_project.ecommerce_saude.pagamentos",
                 "columns": ["id_pedido", "metodo_pagamento", "valor_pago"]},
            ],
        },
        "rows": [
            {"nome_completo": "Ana Souza", "total_pix": 12450.0},
            {"nome_completo": "Bruno Lima", "total_pix": 9800.0},
            {"nome_completo": "Carla Mendes", "total_pix": 7600.0},
        ],
    },
    "expect": {
        "status": "ok",
        "plan": {
            "must_include": ["text_to_sql"],
            "must_not_include": ["chat_answer"],
            "max_steps": 3,
        },
        "steps": {"text_to_sql": {"ok": True}},
        "answer": {
            "must_mention_any": ["ana", "bruno", "pix"],
            "min_length": 30,
        },
        "artifacts": {
            "must_include_types": ["table"],
        },
    },
}
