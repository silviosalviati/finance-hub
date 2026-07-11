# Avaliação profunda — Finance Voice (Finance Auditor)

**Escopo:** `src/agents/finance_auditor/*` — só o agente Finance Voice/Finance Auditor. Outros agentes (`query_build`, `query_analyzer`, `document_build`, `schema_graph`) ficam fora deste documento.
**Dimensões avaliadas:** Segurança · Produtividade/Performance (incluindo eficiência de tokens/custo de LLM) · Assertividade · Boas práticas de mercado (LangGraph).
**Método:** leitura direta do código atual (não é reaproveitamento de notas antigas). Toda afirmação abaixo tem citação `arquivo:linha`.

## Changelog de implementação

Atualizado a cada item resolvido. Serve como checklist para portar manualmente para o GitLab da empresa (repo separado, sem remote/histórico compartilhado com este).

| Arquivo | Funções |
|---|---|
| `README.md` | — |
| `src/agents/finance_auditor/__init__.py` | `analyze`, `_get_graph`, `_summarize_token_usage`, `_make_checkpointer` 🆕 (checkpointer nativo `SqliteSaver`, resolve 4.1 antigo) |
| `src/agents/finance_auditor/catalog_index.py` | `warmup_catalog_loop` (logs `[catalog_warmup]`/`[gold_metric_sync]` em `key=value`) |
| `src/agents/finance_auditor/capabilities.py` | `_get_cached_schema` 🆕, `_get_cached_catalog_search` 🆕, `_pick_relevant_tables`, `cap_text_to_sql`, `cap_bq_get_schema`, `cap_catalog_search`; 15 schemas Pydantic de args (`_BqGetSchemaArgs`, `_VizSpecArgs` etc.) 🆕, `_format_validation_error` 🆕, `execute_capability` |
| `src/agents/finance_auditor/supervisor.py` | `node_guardrails_in`, `_resolve_placeholders`, `node_planner`, `node_reflect`, `node_composer`, `node_router`, `node_audit`, `build_supervisor_graph` |
| `src/agents/finance_auditor/supervisor_state.py` | `SupervisorState` (campos `usage_log`, `context_cache`) |
| `src/api/main.py` | `lifespan` (silencia `httpx`/`httpcore`/`google_genai.models` em `WARNING`) |
| `src/api/routes/agents.py` | `runtime_llm_info`, `list_agents` |
| `src/core/database.py` | `_migrate_audit_log_columns` 🆕, `append_finance_audit`, `_CONFIG_DEFAULTS` |
| `src/shared/guardrails/audit.py` | `record` |
| `src/shared/guardrails/injection.py` | `_normalize` 🆕, `check_injection` |
| `src/shared/guardrails/pii_guard.py` | `scrub_for_storage` 🆕, `_artifact_chart_values` 🆕, `_artifact_stats_top_values` 🆕, `apply_guard` |
| `src/shared/guardrails/rbac.py` | `_strict_mode` |
| `src/shared/tools/llm.py` | `_record_usage` 🆕, `summarize_usage_by_label` 🆕, `_usage_log_line` 🆕, `TokenBudgetExceeded` 🆕, `_check_token_budget` 🆕, `create_llm`, `invoke_with_retry`, `invoke_with_retry_async` |
| `static/css/style.css` | `.badge-full-access`, `.modal-field-hint` |
| `static/index.html` | — |
| `static/js/scripts.js` | `_fetchAclMap` 🆕, `_aclHasFullAccess` 🆕, `adminLoadUsers`, `adminOpenUserModal`, `adminSaveUser` |

🆕 = função nova (as demais são funções existentes que foram alteradas).

**Portar para o GitLab:** repositório separado sem remote compartilhado — replicar manualmente usando a tabela acima, ou pedir um `git diff` dos arquivos para aplicar com `git apply` se a base for igual.

---

## Resumo executivo

| Dimensão | Status |
|---|---|
| **Segurança** | ✅ Nenhum achado em aberto — tudo corrigido nesta auditoria (ver Changelog). |
| **Produtividade/Performance** | ⚠️ Pendências: prompt do planner sem cache de contexto (específico do Vertex), streaming real. |
| **Assertividade** | ✅ Nenhum achado em aberto — tudo corrigido ou decidido (ver Changelog). |
| **Boas práticas LangGraph** | ⚠️ Capabilities fora do padrão `bind_tools`/`ToolNode` (decisão deliberada) e estado sem reducers. Checkpointer nativo já resolvido (ver Changelog). |

---

## 1. Segurança

Nenhum achado em aberto nesta rodada — todos implementados, ver Changelog no topo do arquivo.

---

## 2. Produtividade / Performance

### 🟡 Médio

**2.6 — Sem streaming real de tokens**
`graph.stream(...)` é consumido internamente até o fim e devolvido como um único JSON (`__init__.py:77-78`); o efeito de "digitação" no frontend (`_faRevealText`, `scripts.js:5296-5349`) é sintético, sobre uma resposta já pronta. Para um produto de "voz financeira" conversacional, isso é abaixo do padrão de mercado atual para chat com LLM.

### 🟠 Eficiência de tokens / custo de LLM (achados novos desta rodada)

**2.7 — Prompt do planner sem cache de contexto: ~3,7k tokens estáticos reenviados em toda chamada**
`PLANNER_PROMPT` (`supervisor_prompts.py:7-281`) tem 14.653 caracteres (~3.700 tokens) — embute o catálogo completo das 14 capabilities com args e exemplos, mesmo quando o plano final usa só 1-2 delas. Esse bloco é praticamente idêntico entre chamadas (só a data muda, `supervisor.py:283-285`), mas é reenviado e retokenizado do zero em toda invocação do planner, inclusive para follow-ups triviais ("obrigado", "e no mês passado?"). Não há uso de Vertex AI Context Caching em lugar nenhum do projeto (`grep` por `cached_content`/`context_cach`/`CachedContent`/`cache_control`/`prompt_caching` em `src/` não encontra nada). Essa é a maior alavanca de economia disponível: um turno típico já gasta ~4,5k tokens só de entrada no planner.

> **Progresso (2026-07-11):** o modelo default (`gemini-2.5-flash`, `llm.py:55`) tem limiar baixo de cache (~2k tokens) e o prompt já está estruturado do jeito ideal pro cache implícito do Vertex (bloco estático como `SystemMessage`, antes do conteúdo variável — `supervisor.py:283-325`; `__DATE_BLOCK__` muda só 1x/dia, então o prompt fica byte-idêntico entre chamadas do mesmo dia). Faltava visibilidade: `response.usage_metadata.cached_content_token_count` já vinha da lib mas era descartado. Instrumentado agora (`_record_usage`/`_usage_log_line` em `llm.py`, `_summarize_token_usage` em `__init__.py`) — `cache_read_tokens` passa a aparecer no log `[llm_timing]` e no `token_usage_json` do audit. **Ainda não confirmado em produção se o cache está de fato ativo** — item continua aberto até essa confirmação (ou até implementar cache explícito via `cached_content=`, se o implícito não pegar).

---

## 3. Assertividade (qualidade e confiabilidade das respostas)

### Pontos fortes a reconhecer
- **Loop planner → reflect → composer** bem desenhado: `reflect` critica o plano executado e pode adicionar passos de correção, limitado a `_MAX_ITERATIONS=2` (`supervisor.py:65`) e `_MAX_PLAN_STEPS=6` (`:64`) — nunca degenera em loop infinito, mas ainda dá uma segunda chance real.
- **Fuzzy-matching para alucinação de nomes**: `_fuzzy_pick_dataset`/`_fuzzy_pick_column` (`capabilities.py:144-200`) recuperam dataset/coluna quando o LLM erra o nome exato — reduz respostas "não encontrei" desnecessárias.
- **`_normalize_plan_steps`** (`supervisor.py:233-268`) corrige preventivamente um erro comum do planner (chamar `metric_execute` sem `key`) antes mesmo de executar.
- **Retry com feedback de erro anexado**: quando `text_to_sql` falha, o erro/SQL tentado é anexado de volta ao prompt via `_attach_retry_feedback` (`supervisor.py:585-616`) para a próxima tentativa — não é um retry cego.
- **Erro de capability nunca vira exceção não tratada**: `execute_capability` (`capabilities.py:1698-1704`) retorna erro estruturado mesmo se o planner alucinar um nome de capability inexistente.

---

## 4. Boas práticas de mercado (LangGraph)

### ✅ Acertos
- **Guarda de max-iterations correta** — evita exatamente o anti-padrão "loop infinito sem saída" que a própria documentação de LangGraph adverte. `_reflect_router` sempre converge para `"composer"`.
- **Uso de state para fluxo de dados** — nenhum nó depende de estado externo escondido; tudo passa pelo `SupervisorState`, seguindo o padrão recomendado (evita o anti-padrão "nós sem estado").

### 🟠 Divergências da idiomática LangGraph (dívida arquitetural)

**4.1 — Capabilities não são tools LangChain (`bind_tools`/`ToolNode`)**
`CAPABILITY_REGISTRY` (`capabilities.py:1679-1695`) é um dicionário nome→função, dirigido por um plano gerado via structured output — não pelo mecanismo nativo de tool-calling do LangChain (o LLM nunca vê um schema JSON de tool, só uma descrição em texto livre no prompt, `supervisor_prompts.py:15-273`). Essa é uma escolha de arquitetura deliberada (plan-and-execute com DAG e paralelismo interno via `ThreadPoolExecutor`, em vez de ReAct passo-a-passo) e **não é "errada"** — mas diverge do padrão que o próprio ecossistema LangGraph documenta como recomendado, perdendo validação automática de schema de tool-call e compatibilidade com tooling que espera `tool_calls` nativos (ex.: tracing de tool-use no LangSmith fica menos estruturado).

**4.2 — Estado 100% monolítico, sem reducers**
`SupervisorState` (`supervisor_state.py:8-52`) é um único `TypedDict(total=False)` plano, sem nenhum `Annotated[..., reducer]` — nem `add_messages`, nem reducer customizado. Hoje isso não causa bug porque a execução é single-threaded por invocação (o paralelismo do `node_router` é interno via `ThreadPoolExecutor`, não paralelismo de nós do grafo). Mas é o anti-padrão "estado gigante monolítico" citado pela própria skill de LangGraph — se o grafo algum dia ganhar branches paralelos de verdade, campos tipo `tool_results`/`warnings`/`plan` (hoje sobrescritos inteiros a cada nó) vão colidir silenciosamente sem um reducer.

---

## Nota final

Este documento é atualizado conforme itens são implementados: cada correção aplicada sai da análise e vira uma entrada no "Changelog de implementação" no topo do arquivo, com os arquivos alterados. Itens já implementados ou com decisão final tomada (fix ou não-fix) saem do corpo do relatório. A decisão de quais achados atacar em seguida, e em que sprint, continua com o usuário.
