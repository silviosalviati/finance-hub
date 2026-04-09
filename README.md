# Finance Hub

Plataforma de assistentes de dados com backend FastAPI e frontend web para fluxos de SQL no BigQuery.

## Objetivo

O projeto centraliza assistentes para:

- analisar e otimizar queries existentes (Query Analyzer)
- construir queries a partir de linguagem natural (Query Build)
- evoluir novos agentes de dados (ex.: Document Build, Finance Auditor)

## Arquitetura

```text
bot-query/
├── src/
│   ├── api/                 # FastAPI app, rotas e dependencias
│   ├── agents/              # Agentes por dominio
│   ├── core/                # Contratos base, registry e checkpoint
│   └── shared/              # Config, ferramentas BigQuery/LLM e utilitarios
├── static/                  # Frontend (HTML/CSS/JS)
├── tests/                   # Testes automatizados
├── scripts/                 # Automacoes de dev/publish
├── requirements.txt
└── README.md
```

Arquivos de referencia:

- [src/api/main.py](src/api/main.py)
- [src/api/routes/agents.py](src/api/routes/agents.py)
- [src/api/routes/auth.py](src/api/routes/auth.py)
- [src/api/dependencies.py](src/api/dependencies.py)
- [src/shared/config.py](src/shared/config.py)
- [scripts/publish.ps1](scripts/publish.ps1)

## Modulos de Agentes

### Query Analyzer

- entrada: SQL existente + project_id
- saida: score, diagnostico, query otimizada, recomendacoes e custo estimado
- rota principal: `POST /analyze`

### Query Build

- entrada: solicitacao em linguagem natural + project_id + dataset_hint opcional
- saida: query construida, explicacao, dry-run e recomendacoes de uso eficiente
- rota principal: `POST /api/agents/query_build/analyze`

### Document Build

- placeholder para evolucao futura

## Fluxo Tecnico (alto nivel)

1. Frontend envia request para API.
2. API valida sessao e payload.
3. Registry resolve o agente.
4. Agente executa grafo (LangGraph) com LLM compartilhada.
5. Ferramentas de BigQuery executam dry-run/custo.
6. Resultado e checkpoint sao retornados/salvos.

## Requisitos

- Python 3.10+
- ambiente virtual ativo
- credenciais GCP validas para BigQuery
- acesso a um provider de LLM (OpenAI, Vertex AI ou Hugging Face)

## Instalacao

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Variaveis de Ambiente

Preencha [.env](.env) com as variaveis usadas em [src/shared/config.py](src/shared/config.py):

Obrigatorias:

- `LLM_PROVIDER` (`openai`, `vertexai` ou `huggingface`)
- `GCP_PROJECT_ID`
- `GOOGLE_APPLICATION_CREDENTIALS`

Opcional por provider:

- OpenAI: `OPENAI_API_KEY`, `OPENAI_MODEL`
- Vertex: `VERTEXAI_PROJECT`, `VERTEXAI_LOCATION`, `VERTEXAI_MODEL`
- Hugging Face: `HF_API_TOKEN`, `HF_MODEL_ID`, `HF_ENDPOINT_URL`, `HF_MAX_NEW_TOKENS`, `HF_TEMPERATURE`

Sessao e runtime:

- `SESSION_TTL_HOURS`
- `ALLOWED_ORIGINS` (lista separada por virgula)
- `BQ_COST_PER_TB_USD`
- `BYTES_WARNING_THRESHOLD`
- `BYTES_CRITICAL_THRESHOLD`

Usuarios da aplicacao:

- recomendado: `APP_USERS` no formato `usuario:senha_ou_hash:nome`
- fallback: `APP_USERNAME`, `APP_PASSWORD`, `APP_NAME`

Exemplo:

```env
LLM_PROVIDER=openai
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-4o
GCP_PROJECT_ID=meu-projeto
GOOGLE_APPLICATION_CREDENTIALS=secrets/credentials.json
APP_USERS=analista:$2b$12$hash_bcrypt_aqui:Analista Dados
```

## Como executar

Opcao recomendada:

```powershell
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Opcao alternativa:

```powershell
python src/api/main.py
```

Acesse no navegador:

- http://localhost:8000

## Endpoints Principais

Publicos:

- `GET /`
- `GET /health`
- `GET /favicon.ico`

Autenticacao:

- `POST /api/login`
- `POST /api/logout`
- `GET /api/me`

Agentes:

- `GET /api/agents`
- `GET /api/runtime-llm`
- `POST /analyze` (atalho para Query Analyzer)
- `POST /api/agents/{agent_id}/analyze`
- `GET /api/agents/{agent_id}/checkpoint`

## Frontend

Arquivos principais:

- [static/index.html](static/index.html)
- [static/css/style.css](static/css/style.css)
- [static/js/scripts.js](static/js/scripts.js)

## Testes

Executar:

```powershell
pytest -q
```

Testes atuais:

- [tests/agents/test_query_analyzer.py](tests/agents/test_query_analyzer.py)
- [tests/agents/test_query_build.py](tests/agents/test_query_build.py)
- [tests/agents/test_document_build.py](tests/agents/test_document_build.py)
- [tests/shared/test_bigquery_tools.py](tests/shared/test_bigquery_tools.py)

## Processo de Commit e Publicacao

Padrao recomendado (commit + push automaticos):

```powershell
.\scripts\publish.ps1 -Message "feat: descricao da alteracao"
```

Pulando testes (somente quando necessario):

```powershell
.\scripts\publish.ps1 -Message "chore: ajuste rapido" -SkipTests
```

Esse script executa:

1. validacao da branch atual
2. `pytest -q` (padrao)
3. `git add -A`
4. `git commit -m "..."`
5. `git push origin <branch-atual>`

## Checkpoints e Arquivos Locais

- checkpoints de agentes sao salvos em `.sixth/checkpoints`
- em ambiente de desenvolvimento, revise se deseja versionar esse diretorio

## Roadmap Curto

- ampliar cobertura de testes por fluxo de agente
- adicionar observabilidade de custo e latencia por request
- evoluir Document Build e Finance Auditor
