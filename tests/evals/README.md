# Finance Voice IA — Eval Harness

Bateria de regressão **determinística** que roda o grafo real do agente com
LLM e BigQuery completamente stubados. Detecta regressão de prompt, de
roteamento do Planner, do Composer (anti-meta-resposta, nomes técnicos) e
das capabilities — sem gastar 1 token nem 1 byte BQ.

## Como rodar

```bash
# pytest (parametriza um teste por case)
pytest tests/evals

# CLI interativa (cores + detalhes)
python -m tests.evals.runner

# Um case específico
python -m tests.evals.runner --case 01

# Relatório JSON
python -m tests.evals.runner --json out.json
```

## Estrutura de um case

Cada arquivo `cases/case_*.py` exporta um dict `CASE`. Quatro blocos:

| Bloco | O que define |
|---|---|
| `question / project_id / user / user_profile / attachments` | A entrada que o agente receberia |
| `script` | Respostas pré-gravadas do LLM por chamada (plano, picker, sql, reflect, composer) |
| `bq` | Datasets, tabelas e linhas que as tools BQ devolvem |
| `expect` | Assertions declarativas (plano, steps, answer, artifacts, persona) |

### Schemas de assertion suportados

```python
"expect": {
    "status": "ok",                      # response.status
    "persona": "diretor",                # detect_persona
    "plan": {
        "must_include": ["text_to_sql"], # capabilities obrigatórias
        "must_not_include": ["chat_answer"],
        "min_steps": 1, "max_steps": 3,
    },
    "steps": {
        "text_to_sql": {"ok": True},     # status do step específico
    },
    "answer": {
        "must_mention_any": ["pix"],
        "must_mention_all": [],
        "must_not_mention": ["limitação interna", "tente refazer"],
        "min_length": 30,
    },
    "artifacts": {
        "must_include_types": ["table"],
        "must_not_include_types": ["schema"],
    },
}
```

## Cases atuais (golden battery)

| # | Cobre |
|---|---|
| 01 | Pergunta analítica vira `text_to_sql`, não `chat_answer` |
| 02 | Saudação vira `chat_answer` |
| 03 | `dataset_ref` errado é fuzzy-resolvido (`ecommerce` → `ecommerce_saude`) |
| 04 | Composer não usa frases banidas (anti-meta-resposta) |
| 05 | Persona Diretor detectada por texto |
| 06 | Composer não vaza nomes de project/dataset/tabela |
| 07 | SQL trivial/placeholder rejeitado pelo guard |

## Adicionando um case novo

1. Copie um arquivo existente como modelo: `cp cases/case_01_analytical_pix.py cases/case_NN_seu_nome.py`
2. Ajuste `CASE["id"]`, `question`, `script` e `bq` para o cenário
3. Liste o que você espera em `expect`
4. Rode `python -m tests.evals.runner --case NN` para inspecionar
5. Commit + push — a CI passa a vigiar essa regressão para sempre

## Próximos passos (futuros)

- **`--live`** — executar contra LLM e BigQuery reais (custo controlado, fora da CI rápida).
- **LLM-as-judge** — para qualidade narrativa subjetiva (concisão, tom).
- **Métrica agregada** — % de cases verde por categoria (planner / composer / capability).
- **Diff visual** — comparar relatório atual vs. baseline para "Tests Lab" no portal admin.
