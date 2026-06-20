# Finance Voice IA — Topologia Completa e Profunda

> **Estado em `main` @ `92cf89a`** · 10 nós no grafo · 15 capabilities · 215 testes
> Gerado a partir de inspeção direta do código.

---

## Sumário

1. [Visão geral em 30 segundos](#1-visão-geral-em-30-segundos)
2. [O grafo LangGraph nó-a-nó](#2-o-grafo-langgraph-nó-a-nó)
3. [Capability Registry (15 capabilities)](#3-capability-registry)
4. [Camadas de governança](#4-camadas-de-governança)
5. [Camadas de memória](#5-camadas-de-memória)
6. [Persistência (SQLite + BigQuery)](#6-persistência)
7. [LLM e structured output](#7-llm-e-structured-output)
8. [Frontend e API](#8-frontend-e-api)
9. [Eval Harness](#9-eval-harness)
10. [Fluxos canônicos passo-a-passo](#10-fluxos-canônicos)
11. [Decisões arquiteturais conscientes](#11-decisões-arquiteturais)
12. [Pontos cegos conhecidos](#12-pontos-cegos)

---

## 1. Visão geral em 30 segundos

Finance Voice IA é um **agente conversacional de análise de dados sobre BigQuery**, construído como um **Plan-and-Execute LangGraph** com 10 nós, augmentado por:

- **Reflect loop** com 1 retry baseado em "plano incompleto OU step falhou".
- **Capabilities autônomas** — `text_to_sql` lista tabelas, escolhe via LLM, busca schemas e gera SQL sozinho.
- **Late binding** `${PROJECT}` / `${step_N.payload.path[i]}` no router.
- **Governança ponta-a-ponta**: RBAC por dataset/métrica, PII guard na saída, audit log obrigatório.
- **Catalog RAG** com embeddings (Vertex `text-embedding-005` + pgvector-lite em SQLite).
- **Semantic Layer** de métricas governadas (CRUD via API admin).
- **Personas** (Coordenador / Gerente / Diretor / Geral) + **response modes** (padrão / análise profunda).
- **Eval harness** com 7 cases golden rodando o grafo inteiro determinisísticamente.

```
USER → guardrails_in → persona → response_mode → planner → router ↔ reflect
                                                                    │
                              ┌─────────────────────────────────────┘
                              ▼
                            composer → audit → guardrails_out → USER
```

---

## 2. O grafo LangGraph nó-a-nó

### Topologia exata (do `build_supervisor_graph`)

```
START
  ↓
guardrails_in            ◀── 🛡️ injeção / PII pré-input
  ↓
persona_resolver         ◀── 🧭 Coord / Gerente / Diretor / Geral
  ↓
response_mode_resolver   ◀── 🎚️ padrao | analise_profunda
  ↓
planner                  ◀── 🧠 LLM + PlanResponse Pydantic
  ↓
router  ◀──┐             ◀── 🔧 executa cada step com late binding
  ↓        │
reflect    │             ◀── 🪞 LLM + ReflectVerdict
  ├─ inválido + steps + iter<2 ──→ apply_reflect_plan ──┘
  ├─ válido / esgotou retries
  ↓
composer                 ◀── 🎨 LLM criativo, persona + modo
  ↓
audit                    ◀── 📋 finance_audit_log
  ↓
guardrails_out           ◀── 🛡️ PII mask/block/off
  ↓
END
```

### Cada nó em detalhe

#### `guardrails_in` (security)
- Regex contra `ignore previous`, `desconsidere as instruções`, etc.
- Saída: `{guardrail_in_ok: bool, guardrail_in_reason: str}`.
- Se bloqueado: composer e guardrails_out ainda rodam mas o final_answer já vem com mensagem padrão.

#### `persona_resolver` (deterministic)
- Heurística regex em `personas.py`:
  - `\bdiretor`, `visão executiva`, `roi`, `impacto financeiro` → **diretor**
  - `\bgerent`, `tático`, `KPI`, `MoM`, `YoY` → **gerente**
  - `coordenador`, `operacional`, `drill-down`, `top N`, `acionável` → **coordenador**
  - default → **geral**
- **Profile sticky**: se a sessão tem `profile.persona` válida, ela vence.
- Saída: `{persona: str}`.

#### `response_mode_resolver` (deterministic)
- Detecta termos como `análise profunda`, `relatório completo`, `vai fundo`.
- Modos: `padrao` (default) ou `analise_profunda`.
- Saída: `{response_mode: str}`. Composer adapta verbosidade.

#### `planner` (LLM structured)
- `llm.with_structured_output(PlanResponse)` → `{rationale, steps[]}`.
- Cada `PlanStep` = `{capability, args, rationale}`.
- **Regras-chave no prompt:**
  - Plano DEVE terminar em capability "answer-producing" (`text_to_sql`, `bq_query`, `metric_execute`, `stats_describe`, `forecast_simple`, `attachment_analyze`).
  - `chat_answer` proibido para perguntas sobre entidades de negócio.
  - Para domínio fuzzy ("ecommerce de saúde") → plano de 1 step com `text_to_sql` + `dataset_ref`.
  - Late binding via `${PROJECT}` / `${step_N.payload.path}`.
- Limite: `_MAX_PLAN_STEPS = 6`.

#### `router` (orchestrator)
- Para cada step do plan:
  1. **Resolve placeholders** (`_resolve_placeholders`) — substitui `${PROJECT}` e `${step_N.path}` por valores reais.
  2. Injeta `context`: `request_text`, `project_id`, `llm`, `llm_creative`, `user`, `attachments`, `tool_results` (steps anteriores).
  3. Chama `execute_capability(capability, args, context)`.
  4. Registra `{step_index, capability, args, ok, payload, error}` em `tool_results`.
  5. Coleta `artifacts` (tabelas, sql, schemas, vega-lite, stats, etc.).
- **Preserva tool_results entre iterações** — em retry só executa o "rabo" do plano estendido.
- Incrementa `iteration`.

#### `reflect` (LLM structured)
- Heurística primeiro:
  - **Se algum step falhou OU plano não tem answer-producer** → tenta refletir.
  - Senão → skip (não gasta LLM).
- `llm.with_structured_output(ReflectVerdict)` → `{is_valid, confidence, issues, suggested_steps[]}`.
- Limite: `_MAX_ITERATIONS = 2`.

#### `apply_reflect_plan` → `router` (conditional loop)
- Anexa `suggested_steps` ao plano e devolve ao router.
- Sem novos prompts ao planner — reflect já produziu os steps prontos.

#### `composer` (LLM creative)
- Template injeta:
  - Bloco de **persona** (formato esperado por altitude).
  - Bloco de **response_mode** (verbosidade extra para análise profunda).
  - Lista resumida de `tool_results` (payloads truncados).
- **Regras anti-meta-resposta** no prompt:
  - Proibido: "tente refazer", "limitação interna", "problema técnico", "consulta que seria executada".
  - Proibido: vazar nomes de projeto/dataset/tabela/coluna no texto.
  - Proibido: imprimir `attempted_sql` quando o step falhou.

#### `audit` (persistence)
- `audit.record(state)` → escreve em `finance_audit_log`:
  - `user_id`, `persona`, `request_text`, `plan_json`, `steps_total`, `steps_ok`, `bytes_processed`, `estimated_cost_usd`, `error`, `ts`.
- Falhas no audit nunca derrubam o fluxo (`try/except` envolvem o write).

#### `guardrails_out` (security)
- `pii_guard.apply_guard(final_answer, artifacts)`:
  - Regex pt-BR para CPF/CNPJ/email/telefone/cartão.
  - Modo `mask` (default): preserva últimos 4 dígitos → `[CPF_***-09]`.
  - Modo `block`: bloqueia resposta inteira se detectar PII.
  - Modo `off`: passthrough.
- Aplica recursivamente em `final_answer` e em `rows`/`sql`/`text` dos artefatos.
- Adiciona warning quando há ocorrências.

---

## 3. Capability Registry

15 capabilities registradas em `CAPABILITY_REGISTRY` (dispatch via `execute_capability(name, args, context)`):

### 📥 Descoberta
| Capability | O que faz | Auto-correção |
|---|---|---|
| `bq_list_datasets` | Lista datasets do projeto | — |
| `bq_list_tables` | Lista tabelas de um dataset | Fuzzy fallback se `dataset_hint` não existe (slug + substring + difflib ≥ 0.4) |
| `bq_get_schema` | Schema de uma tabela | Deriva project_id do `table_ref` |
| `catalog_search` | **RAG semântico** sobre tabelas do projeto (embeddings) | TTL automático, reindex on demand |

### 🎯 Answer-producing
| Capability | O que faz | Detalhe arquitetural |
|---|---|---|
| `bq_query` | Executa SQL livre (SELECT/WITH) | Dry-run + budget (5 GiB) + guard contra trivial |
| `text_to_sql` | NL → SQL → executa | **Autônomo**: aceita `dataset_ref` e sozinho lista tabelas, **picker LLM** escolhe relevantes, busca schemas, gera SQL via `with_structured_output(SqlOutput)`, valida, executa. Fuzzy de `dataset_ref` também. |
| `metric_execute` | Roda métrica do Semantic Layer | Renderiza `{date_start}` / `{date_end}` / `{limit}` no template |
| `stats_describe` | Estatística descritiva | mean/median/stdev/quartis (numérico) + top-k (categórico). Stdlib-only |
| `viz_spec` | Spec Vega-Lite | bar/line/area/point/arc com inferência de tipo |
| `forecast_simple` | Projeção linear | OLS pure-Python · slope/R²/horizonte |
| `attachment_analyze` | Analisa anexos | CSV (stdlib) ou imagem (Gemini multimodal) |
| `org_fact_recall` | Recupera memória organizacional | Token overlap em `finance_org_facts` |

### 🧭 Suporte / Governança
| Capability | O que faz |
|---|---|
| `metric_lookup` | Busca métricas governadas (lexical com stemming pt-BR + bônus) |
| `org_fact_save` | Persiste fato (escopo `user` ou `global`; non-admin é demovido) |
| `chat_answer` | Fallback social (sem dados) |

### Contrato uniforme

```python
def cap_xyz(args: dict, context: dict) -> dict:
    return {
        "ok": bool,
        "payload": dict | list | None,
        "error": str | None,
        "artifacts": list[dict],  # type ∈ {table, sql, schema, vega_lite, stats, ...}
    }
```

`context` carrega: `request_text`, `project_id`, `dataset_hint`, `llm`, `llm_creative`, `user`, `attachments`, `tool_results` (steps anteriores).

---

## 4. Camadas de governança

```
┌────────────────────────────────────────────────────────────────────────┐
│                          GOVERNANCE LAYER                              │
├──────────────────────┬──────────────────────┬─────────────────────────┤
│       RBAC           │     PII GUARD        │      AUDIT TRAIL        │
│                      │                      │                         │
│ • allowed_datasets   │ Regex pt-BR:         │ finance_audit_log:      │
│ • allowed_metrics    │ • CPF / CNPJ         │ • user · persona        │
│ • denied_datasets    │ • email · telefone   │ • plan_json · steps     │
│                      │ • cartão de crédito  │ • bytes BQ · USD        │
│ Wildcard "log_*"     │                      │                         │
│ Admin bypass         │ Modos:               │ Endpoint admin:         │
│ Strict mode opcional │ • mask (default)     │ GET /admin/finance/     │
│                      │ • block              │     audit               │
│ Aplicado em:         │ • off                │                         │
│ • bq_list_tables     │                      │ Endpoint trigger:       │
│ • bq_get_schema      │ Aplicado em:         │ POST /admin/finance/    │
│ • bq_query           │ • final_answer       │      alerts/run         │
│ • text_to_sql        │ • artifact.rows      │                         │
│ • metric_execute     │ • artifact.sql/text  │ (cron-friendly)         │
│ • catalog_search     │                      │                         │
└──────────────────────┴──────────────────────┴─────────────────────────┘
                                  │
                                  ▼
                  ┌────────────────────────────────┐
                  │     SEMANTIC LAYER             │
                  │                                │
                  │  finance_semantic_metrics:     │
                  │  • key + name + description    │
                  │  • source_table                │
                  │  • sql_template                │
                  │    (placeholders {date_start}, │
                  │     {date_end}, {limit})       │
                  │  • alert_threshold (JSON DSL)  │
                  │  • tags + owner                │
                  │                                │
                  │  Lookup lexical com:           │
                  │  • stemming pt-BR              │
                  │  • bônus por substring         │
                  │  • peso por campo (key>name>   │
                  │    tags>description)           │
                  │                                │
                  │  Admin endpoints:              │
                  │  • PUT/DELETE /metrics/{key}   │
                  │  • GET /metrics                │
                  └────────────────────────────────┘
```

---

## 5. Camadas de memória

```
┌─────────────────────────────────────────────────────────────────────┐
│                         MEMORY STACK                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  📌 SESSION (curta)        : Checkpointer SQLite                    │
│  ────────────────────       ─────────────────────                   │
│  • profile.name             • Extraído via regex                     │
│  • profile.persona sticky   • Reaproveitado pelo persona_resolver    │
│  • turns[] (RAG lexical)    • Detecção de pergunta repetida          │
│                                                                      │
│  📚 ORGANIZATIONAL (longa) : finance_org_facts (SQLite)             │
│  ────────────────────────   ──────────────────────────              │
│  • Por user_id ou global    • CRUD via /admin/finance/org-facts     │
│  • Salva via cap_org_fact_  • Busca via cap_org_fact_recall          │
│    save (LLM ou admin)      • Recall = token overlap                 │
│                                                                      │
│  🧠 SEMANTIC (catálogo)    : finance_catalog_index (SQLite + vec)   │
│  ─────────────────────      ───────────────────────────────────     │
│  • Embeddings text-embed-005• 1 entry por tabela                     │
│  • Reindex com TTL          • Cosine similarity puro Python         │
│  • Project-scoped           • RAG sobre nomes/desc/colunas           │
│                                                                      │
│  📋 EXECUTION (auditoria)  : finance_audit_log (SQLite)             │
│  ─────────────────────      ──────────────────────────              │
│  • 1 entrada por run        • user · persona · plan · bytes · USD   │
│  • Índice user_id, ts DESC  • Base potencial para query-history RAG │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 6. Persistência

### SQLite (`.sixth/app.db`)

| Tabela | Conteúdo |
|---|---|
| `users` | id, username, password_hash, name, is_admin, timestamps |
| `app_config` | key/value/description/updated_at/updated_by |
| `query_analyzer_memory` | padrões cross-sessão por dataset (outro agente) |
| **`finance_semantic_metrics`** | key, name, description, source_table, sql_template, owner, tags, alert_threshold |
| **`finance_org_facts`** | id, user_id, scope (user\|global), fact_text, tags |
| **`finance_user_acl`** | user_id, allowed_datasets, allowed_metrics, denied_datasets |
| **`finance_audit_log`** | id, ts, user_id, persona, request_text, plan_json, steps_total, steps_ok, bytes_processed, estimated_cost_usd, error |
| **`finance_catalog_index`** | project_id, dataset_id, table_id, summary, embedding_json, updated_at |

### BigQuery
- Tools em `src/shared/tools/bigquery.py`: `dry_run_query`, `execute_query_rows`, `get_dataset_tables_metadata`, `get_table_schema`, `_list_project_datasets`, `_get_client`.
- **Sempre** `dry_run` antes de execute. Budget global: `FINANCE_AUDITOR_QUERY_BUDGET_BYTES = 5 GiB` (configurável).

### Vertex AI
- LLM analítico (planner, sql, reflect): `gemini-2.5-flash` @ temp 0.05.
- LLM criativo (composer, multimodal): `gemini-2.5-flash` @ temp 0.30.
- Embeddings (catalog_search): `text-embedding-005`.

---

## 7. LLM e structured output

Toda comunicação crítica com o LLM usa **`with_structured_output(Pydantic)`** — elimina parsing frágil.

| Schema | Onde | Para que |
|---|---|---|
| `PlanResponse` | planner | `{rationale, steps[]}` |
| `PlanStep` | planner + reflect | `{capability, args, rationale}` |
| `ReflectVerdict` | reflect | `{is_valid, confidence, issues, suggested_steps[]}` |
| `_SqlOutput` (privado) | `cap_text_to_sql` | `{sql}` — Vertex garante shape válido |
| `_Picked` (privado) | `_pick_relevant_tables` | `{table_ids[], rationale}` |

**Único nó com plain `invoke` (sem structured):** `composer` — porque a saída é markdown livre.

**Late binding** (`router._resolve_placeholders`):
- `${PROJECT}` → `project_id` do contexto
- `${step_N.payload.key.subkey[0]}` → caminha o JSON do step anterior
- Steps falhos não substituem (mantém token literal — sinal de falha visível)

---

## 8. Frontend e API

### Endpoints (FastAPI)

```
POST /api/agents/finance_auditor/analyze    ◀── chat principal
     body: {query, project_id, dataset_hint?, attachments?, thread_id?}
     returns: {status, response_mode, persona, plan, plan_rationale,
               tool_results, artifacts, markdown_report, chat_answer,
               warnings, pii, audit_id}

ADMIN /admin/finance/...
  GET    /metrics                     ◀── lista Semantic Layer
  PUT    /metrics/{key}               ◀── CRUD métrica
  DELETE /metrics/{key}
  GET    /acl  ·  /acl/{user_id}      ◀── RBAC
  PUT    /acl/{user_id}
  GET    /audit                       ◀── audit log
  GET    /org-facts                   ◀── memória organizacional
  POST   /org-facts
  DELETE /org-facts/{fact_id}
  POST   /alerts/run                  ◀── trigger de alertas
  POST   /catalog/reindex             ◀── reindexa catalog_index
```

### Frontend (HTML/JS, `static/`)
- **Status bar enxuta**: chip ✓/✗ apenas (hover mostra custo agregado).
- **Painel "Detalhes da execução" removido** — nada de internals expostos.
- **Artefatos filtrados** — só renderiza saída de capabilities answer-producing (`text_to_sql`, `bq_query`, `metric_execute`, `stats_describe`, `viz_spec`, `forecast_simple`, `attachment_analyze`, `org_fact_recall`). SQL/schema técnicos ficam escondidos.
- **SQL com highlight** dark + botão "copiar" via data-attribute (sem injeção possível).
- **Tabelas** com sticky header + banded rows.
- **Vega-Lite** auto-embed via `vegaEmbed` (fallback para JSON pretty-print).

---

## 9. Eval Harness

`tests/evals/` — bateria de regressão **determinística** com 7 cases golden.

```
tests/evals/
├── runner.py                  ScriptedLLM + stubs BQ + assertions DSL + CLI
├── test_eval_cases.py         pytest parametriza 1 teste por case
├── README.md                  doc do DSL + como adicionar case
└── cases/
    ├── case_01_analytical_pix.py            (pergunta → text_to_sql)
    ├── case_02_chat_greeting.py             (saudação → chat_answer)
    ├── case_03_dataset_typo_fuzzy.py        (fuzzy "ecommerce"→"ecommerce_saude")
    ├── case_04_anti_meta_response.py        (proíbe "tente refazer", etc.)
    ├── case_05_persona_diretor.py           (persona é propagada)
    ├── case_06_no_internal_names_leak.py    (sem vazamento de nomes técnicos)
    └── case_07_trivial_sql_blocked.py       (SELECT 'erro' rejeitado)
```

Cada case = dict Python com `question`, `script` (respostas pré-gravadas do LLM), `bq` (dados das tools), `expect` (assertions).

DSL de assertions: `status`, `persona`, `plan.must_include/not_include/min_steps/max_steps`, `steps[cap].ok`, `answer.must_mention_any/all/not_mention/min_length`, `artifacts.must_include/not_include_types`.

---

## 10. Fluxos canônicos

### A. Pergunta analítica simples ("qual cliente paga mais Pix?")

```
USER → "qual cliente paga mais Pix no meu ecommerce de saúde?"
  │
  ▼
guardrails_in   ─── ok ───▶ persona (geral)
  │                              │
  │                              ▼
response_mode (padrao) ───▶ planner
                                 │ LLM produz:
                                 │ {steps: [{
                                 │   capability: text_to_sql,
                                 │   args: {
                                 │     natural_language: "...",
                                 │     dataset_ref: "${PROJECT}.ecommerce_saude"
                                 │   }
                                 │ }]}
                                 ▼
                              router (resolve ${PROJECT})
                                 │
                                 ▼
                              cap_text_to_sql:
                                 1. get_dataset_tables_metadata
                                 2. _pick_relevant_tables (LLM) →
                                    [clientes, pedidos, pagamentos]
                                 3. get_table_schema × 3
                                 4. LLM structured _SqlOutput
                                 5. dry_run + budget check
                                 6. execute_query_rows
                                 ▼
                              reflect: has answer, all ok → composer
                                 │
                                 ▼
                              composer (LLM creative + persona geral)
                                 │
                                 ▼
                              audit (bytes + USD persistidos)
                                 │
                                 ▼
                              guardrails_out (PII mask)
                                 │
                                 ▼
USER ◀── chip ✓ + tabela com top clientes
```

### B. Pergunta que precisa de catalog RAG

```
USER → "qual a margem média por categoria de produto?"
  │
  ▼
... planner gera:
    [
      {capability: catalog_search, args: {query: "margem categoria produto"}},
      {capability: text_to_sql, args: {
         natural_language: "...",
         table_refs: ["${step_0.payload.matches[0].table_ref}",
                      "${step_0.payload.matches[1].table_ref}"]
      }}
    ]
  │
  ▼
router → catalog_search → top-5 tabelas relevantes (embedding RAG)
       → text_to_sql resolvido com tabelas certas
```

### C. Falha + recovery via reflect

```
... router executa text_to_sql → falha (RBAC bloqueou)
  │
  ▼
reflect: any_failed=True, iter=1 < 2
   LLM ReflectVerdict: {
     is_valid: false,
     issues: ["RBAC negou ecommerce_saude"],
     suggested_steps: [{capability: chat_answer}]
   }
  │
  ▼
_reflect_router → "router" (volta com plano estendido)
  │
  ▼
apply_reflect_plan anexa step → router executa chat_answer
  │
  ▼
reflect: agora has_answer → composer entrega resposta explicando
```

---

## 11. Decisões arquiteturais conscientes

| Decisão | Trade-off aceito |
|---|---|
| **Plan-and-Execute** (não ReAct) | Plano fechado upfront é mais previsível e auditável; perde alguma flexibilidade. Reflect loop compensa parcialmente. |
| **Structured output sempre** | Custa 1 LLM call estruturada vs 1 plain text; ganha eliminação de toda categoria de bug de parsing. |
| **Capabilities "fat" (text_to_sql autônomo)** | `text_to_sql` chama LLM 2x internamente (picker + sql) sem aparecer como steps separados; auditabilidade fica reduzida, mas plano fica mais simples e estável. |
| **SQLite para tudo** | Operacionalmente trivial, sem rede; não escala horizontalmente. Trocar por Postgres é não-disruptivo (mesmo schema). |
| **Embeddings em SQLite** | Bom para até ~100k tabelas; cosine similarity em Python puro. Acima disso → pgvector. |
| **PII guard regex pt-BR** | Cobre os 5 tipos comuns (CPF/CNPJ/email/phone/cartão); não substitui DLP corporativo. |
| **Reflect single-shot binário** | `is_valid: bool` + 1 retry. Não distingue "erro técnico" de "ambiguidade semântica". Suficiente para 95% dos casos. |
| **Composer one-shot** | Sem self-consistency; barato e suficiente quando o prompt está bem ajustado. |
| **Eval determinístico stubado** | Roda em todo PR, zero custo; não captura regressão de qualidade subjetiva do LLM real (precisaria modo `--live`). |

---

## 12. Pontos cegos conhecidos

Áreas que **funcionam mas mereceriam evolução**:

| # | Tema | Status atual | O que falta |
|---|---|---|---|
| 1 | **Tool selector** antes do planner | Planner vê todas 15 capabilities sempre | Filtrar top-5 relevantes antes (heurístico ou LLM mini) |
| 2 | **Audit-log como few-shot** | Audit é só read-only para admin | Indexar SQLs bem-sucedidos e injetar no prompt do `text_to_sql` |
| 3 | **Org-facts auto-load** | Só carrega quando planner pede via `org_fact_recall` | Buscar top-3 facts do usuário e injetar no contexto sempre |
| 4 | **Eval `--live`** | Só stub | Modo opcional contra LLM real + LLM-as-judge |
| 5 | **Telemetria fina** | Audit captura agregados | Tokens/latência por nó (LangSmith parcialmente configurado) |
| 6 | **Schema validation de args** | `dict[str, Any]` livre | Pydantic schema por capability + auto-doc para planner |
| 7 | **Reflect typed retry strategies** | Binário | `{retry_same, retry_with_context, ask_user, give_up}` |
| 8 | **MCP** | Tools como funções Python locais | Migrar para MCP server quando capabilities forem compartilhadas com outros agentes |

---

## Apêndice A — Arquivos-chave

```
src/agents/finance_auditor/
├── __init__.py              FinanceAuditorAgent (BaseAgent)
├── supervisor.py            grafo + nós router/reflect/composer/audit/guardrails
├── supervisor_state.py      SupervisorState TypedDict
├── supervisor_schemas.py    PlanResponse, PlanStep, ReflectVerdict, capabilities enum
├── supervisor_prompts.py    PLANNER_PROMPT, REFLECT_PROMPT, COMPOSER_PROMPT_TEMPLATE
├── capabilities.py          15 cap_* + CAPABILITY_REGISTRY + execute_capability
├── personas.py              detect_persona + PERSONA_PROMPTS
├── response_mode.py         detect_response_mode + RESPONSE_MODE_PROMPTS
├── rbac.py                  check_dataset/check_metric (ACL + wildcard + admin)
├── pii_guard.py             scan + scrub_text + apply_guard (mask/block/off)
├── audit.py                 record(state) → finance_audit_log
├── semantic_layer.py        search_metrics + render_sql + resolve_metric
├── catalog_index.py         reindex_catalog + search_catalog (embeddings)
├── org_memory.py            save_fact + recall + forget
├── forecast.py              linear_regression + project (OLS pure Python)
├── multimodal.py            parse_csv + describe_image_with_llm
└── alerting.py              run_alerts (trigger-driven)

src/api/routes/
├── agents.py                /api/agents/{id}/analyze
└── finance_governance.py    /admin/finance/* (CRUD + audit + alerts + catalog)

tests/evals/                 eval harness com 7 cases golden
docs/
└── finance-voice-ia-topology.html   visualização interativa
```

## Apêndice B — Métricas do projeto

| Métrica | Valor |
|---|---|
| Nós no grafo | 10 (+1 conditional router) |
| Capabilities | 15 |
| Personas | 4 (coordenador/gerente/diretor/geral) |
| Response modes | 2 (padrao/analise_profunda) |
| LLM calls por execução típica | 3-4 (planner + picker + sql + composer; reflect só se preciso) |
| Estruturas Pydantic com structured output | 5 |
| Tabelas SQLite proprietárias | 5 (semantic/acl/audit/org_facts/catalog) |
| Endpoints REST | 1 user + 13 admin |
| Testes finance_auditor | 215 (+ 7 evals) |
| Iteração máxima do reflect loop | 2 |
| Budget BQ default por query | 5 GiB |
| PII patterns detectados | 5 (CPF/CNPJ/email/phone/cartão) |
