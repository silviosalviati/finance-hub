# bot-query

Plataforma de bots de dados com FastAPI + frontend web para:

- Query Analyzer (analise e otimizacao de SQL)
- Query Build (geracao de SQL a partir de linguagem natural)
- Document Build (placeholder)

## Arquitetura

```text
project/
├── src/
│   ├── agents/
│   │   ├── query_analyzer/
│   │   ├── query_build/
│   │   ├── document_build/
│   │   └── finance_auditor/
│   ├── shared/
│   │   ├── tools/
│   │   ├── utils/
│   │   └── config.py
│   ├── core/
│   │   ├── base_agent.py
│   │   ├── registry.py
│   │   └── checkpointer.py
│   └── api/
│       ├── main.py
│       ├── dependencies.py
│       └── routes/
│           ├── auth.py
│           └── agents.py
├── static/
├── tests/
├── .env
├── requirements.txt
└── README.md
```

## Requisitos

- Python 3.10+
- Ambiente virtual ativo (.venv)
- Credenciais GCP validas para BigQuery

## Configuracao

Preencha o arquivo [.env](.env) com:

- LLM_PROVIDER e modelo (HF/OpenAI/Vertex)
- Credenciais GCP
- APP_USERS com senha em texto ou hash bcrypt

Exemplo de APP_USERS:

```env
APP_USERS=usuario:$2b$12$hash_bcrypt_aqui:Nome Completo
```

## Instalacao

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Como iniciar

Opcao recomendada:

```powershell
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Opcao alternativa:

```powershell
python src/api/main.py
```

## Endpoints principais

Publicos:

- GET /
- GET /health
- GET /favicon.ico

Auth:

- POST /api/login
- POST /api/logout
- GET /api/me

Agentes:

- GET /api/agents
- GET /api/runtime-llm
- POST /analyze (atalho para query_analyzer)
- POST /api/agents/{agent_id}/analyze
- GET /api/agents/{agent_id}/checkpoint

## Bot Query Build

O Query Build usa o mesmo provider/model configurado globalmente no [.env](.env), com prompt e fluxo dedicados.
Nao e obrigatorio ter uma LLM separada para ele.

## Testes

Arquivos de teste estao em [tests](tests):

- [tests/agents/test_query_analyzer.py](tests/agents/test_query_analyzer.py)
- [tests/agents/test_query_build.py](tests/agents/test_query_build.py)
- [tests/agents/test_document_build.py](tests/agents/test_document_build.py)
- [tests/shared/test_bigquery_tools.py](tests/shared/test_bigquery_tools.py)

Execucao (se pytest estiver instalado):

```powershell
pytest -q
```

## Processo padrao para commitar e subir no GitHub

Use sempre o script abaixo para garantir um fluxo unico: testar, commitar e subir.

Com testes obrigatorios:

```powershell
.\scripts\publish.ps1 -Message "feat: descricao da alteracao"
```

Pulando testes (somente quando necessario):

```powershell
.\scripts\publish.ps1 -Message "chore: ajuste rapido" -SkipTests
```

O script executa:

- Validacao da branch atual
- `pytest -q` (por padrao)
- `git add -A`
- `git commit -m "..."`
- `git push origin <branch-atual>`
