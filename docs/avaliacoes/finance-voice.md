# Avaliação profunda — Finance Voice (Finance Auditor) e agentes LangGraph do Finance Hub

**Data:** 2026-07-09
**Escopo:** `src/agents/finance_auditor/*` (foco principal).
**Dimensões avaliadas:** Segurança · Produtividade/Performance · Assertividade · Boas práticas de mercado (LangGraph).
**Método:** leitura direta do código atual (não é reaproveitamento de notas antigas). Toda afirmação abaixo tem citação `arquivo:linha`. Dá continuidade às auditorias de 2026-06-13 e 2026-06-28 — desta vez com verificação linha a linha do estado presente.

## Changelog de implementação

Atualizado a cada item resolvido. Serve como checklist para portar manualmente para o GitLab da empresa (repo separado, sem remote/histórico compartilhado com este).

| Data | Arquivo | Descrição | Item |
|---|---|---|---|
| 2026-07-09 | `src/core/database.py` | Default de `FINANCE_AUDITOR_RBAC_STRICT`: `"0"` → `"1"` | #1 (RBAC) |
| 2026-07-09 | `src/shared/guardrails/rbac.py` | Fallback de `_strict_mode()` também vira `"1"` (fail-closed); docstring recomenda ACL `"*"` em vez de desligar o strict | #1 (RBAC) |
| 2026-07-09 | `static/index.html` | Checkbox "Acesso total" no modal de usuário do admin; coluna "Acesso" na tabela | #1 (RBAC) |
| 2026-07-09 | `static/js/scripts.js` | `adminLoadUsers`/`adminOpenUserModal`/`adminSaveUser` leem e gravam ACL `"*"` via `/admin/finance/acl/{user}` | #1 (RBAC) |
| 2026-07-09 | `static/css/style.css` | Estilos `.badge-full-access` e `.modal-field-hint` | #1 (RBAC) |
| 2026-07-09 | `src/shared/guardrails/pii_guard.py` | Guard passa a cobrir `spec.data.values` (gráficos) e `columns[].top[].value` (stats) além de `rows`/`sql`/`text`; contagem de PII em `rows` agora entra em `pii_counts` (não entrava antes); nova função `scrub_for_storage()` | 1.2 |
| 2026-07-09 | `src/shared/guardrails/audit.py` | `record()` passa `request_text`/`plan`/`error` por `scrub_for_storage()` antes de persistir no `finance_audit_log` | 1.3 |
| 2026-07-09 | `src/shared/guardrails/injection.py` | Substituídos os 5 substrings literais por ~19 regex com normalização de acentos (`unicodedata`); detecta variações que antes passavam (ex.: "esqueça as instruções") | 1.4 |
| 2026-07-09 | `src/agents/finance_auditor/supervisor.py` | Removida a cópia hardcoded de `_INJECTION_MARKERS`; `node_guardrails_in` agora chama `injection.check_injection()` compartilhado (mesmo guard do query_build) | 1.4 |
| 2026-07-09 | `src/api/routes/agents.py` | `GET /api/runtime-llm` e `GET /api/agents` agora exigem `Depends(get_current_user)`, igual ao resto de `/api/agents/*` | 1.6 |
| 2026-07-09 | `README.md` | Lista de endpoints atualizada: `/api/runtime-llm` movido de "Públicos" para "Protegidos por sessão" | 1.6 |
| 2026-07-09 | `src/agents/finance_auditor/supervisor.py` | `_resolve_placeholders` (late-binding `${step_N.path}`) só resolve valores escalares — dict/list resolvido fica sem substituir em vez de virar `str({...})` cru no arg; resultado limitado a 4000 chars | 1.7 |

**Portar para o GitLab:** repositório separado sem remote compartilhado — replicar manualmente usando a tabela acima, ou pedir um `git diff` dos arquivos para aplicar com `git apply` se a base for igual.

---

## Resumo executivo

| Dimensão | Veredito |
|---|---|
| **Segurança** | Base é sólida (bcrypt, tokens de sessão server-side, SQL guard bem posicionado). Todos os achados desta auditoria (RBAC permissivo, PII em gráficos/stats, PII no audit log, guard de injection duplicado/fraco, endpoints sem auth, late-binding sem validação de tipo) já foram corrigidos em código — ver Changelog. Nenhum item de segurança em aberto nesta rodada. |
| **Produtividade/Performance** | Confiável e com boas fundações (retry+backoff, cache no query_analyzer, tracing via LangSmith), mas com **inconsistência crescente** entre agentes irmãos — os 5 agentes foram claramente escritos em momentos diferentes e não convergiram para os mesmos padrões. |
| **Assertividade** | O núcleo (planner → reflect → composer) é bem desenhado para se autocorrigir, com fuzzy-matching e retry com feedback — acima da média para um agente sem tool-calling nativo. O ponto fraco é a ausência de human-in-the-loop antes de rodar SQL gerado por LLM, algo que os agentes irmãos já têm. |
| **Boas práticas LangGraph** | O grafo é bem contido (guarda de max-iterations correta, sem loop infinito), mas **diverge da idiomática LangGraph em dois pontos estruturais**: não usa checkpointer nativo (perde resume/interrupt) e não usa `bind_tools`/`ToolNode` (reinventa um dispatcher de capabilities). Funciona, mas é dívida técnica de arquitetura, não só de código. |

---

## 1. Segurança

Nenhum achado em aberto nesta rodada — todos implementados, ver Changelog no topo do arquivo.

### 🟢 Verificado como não-problema (para não reabrir como "achado novo")
- `assert_select_only` (SQL guard) está corretamente integrado em todos os pontos que executam SQL gerado por LLM (`capabilities.py:400`, `query_build/nodes.py:232,568`, `alerting.py:24,101`). Único ponto sem o guard é `catalog_index.py:184`, mas ali o SQL é um template fixo (`SELECT * FROM {table_ref}`) com `table_ref` vindo do catálogo real do BigQuery, não de texto do usuário/LLM — não é injetável.
- Sessão: senha com bcrypt em 100% dos caminhos de escrita; token de sessão é opaco (UUID) e validado contra a base a cada request — não há como forjar/escalar via manipulação de token.
- Admin bootstrap gera senha aleatória forte se `ADMIN_DEFAULT_PASSWORD` não for setado — não há credencial fraca hardcoded.
- CORS: `allow_credentials=False`, auth via Bearer token (não cookie) — configuração de `ALLOWED_ORIGINS` aberta não implica roubo de sessão.
- `.sixth/app.db` não é servido por nenhuma rota web — acesso é só a nível de filesystem.

---

## 2. Produtividade / Performance

### 🟠 Alto

**2.1 — `schema_graph` é o único dos 5 agentes sem retry/timeout na chamada LLM**
`src/agents/schema_graph/nodes.py:499` — `llm.invoke(...)` cru, sem `invoke_with_retry`. Todos os outros 4 agentes (query_build, query_analyzer, document_build, finance_auditor) usam o wrapper. Uma falha transitória de rede/quota derruba o enriquecimento inteiro sem tentar de novo.

**2.2 — `schema_graph`: enriquecimento por LLM roda em loop sequencial, apesar do padrão de paralelização já existir no mesmo arquivo**
`enrich_with_llm` (`schema_graph/nodes.py:468-524`) processa lotes de até 30 relações (`_ENRICH_BATCH_SIZE=30`, linha 56) num `for` sequencial (linha 486), uma chamada LLM por lote. No mesmo arquivo, `discover_tables` (linhas 221-230) já usa `ThreadPoolExecutor(max_workers=4)` para I/O de BigQuery — o padrão existe, só não foi aplicado ao enriquecimento por LLM.

**2.3 — `document_build`: dois fetches independentes rodando em sequência sem necessidade**
`fetch_real_schema` (`document_build/nodes.py:130-171`) e `fetch_dataplex_tags` (`:173-248`) são funções síncronas encadeadas como aresta fixa no grafo (`document_build/graph.py:45`). Não há dependência real de dados entre elas (Dataplex só precisa de `table_path`, já resolvido antes). Poderiam rodar em paralelo (thread ou async).

### 🟡 Médio

**2.4 — `max_columns` inconsistente e divergindo cada vez mais**
`get_dataset_tables_metadata` usa default 15 (`src/shared/tools/bigquery.py:350-352`), `get_dataset_tables_schema` usa default 50 (`:389-394`). E cada agente sobrescreve com um valor diferente: `finance_auditor` usa 20, `query_analyzer` usa 20, `document_build` usa 50, `schema_graph` usa 100. Já são **4 valores diferentes** em uso — a divergência cresceu desde a auditoria de 06-28 (era só 15 vs 50).

**2.5 — `_THREAD_TTL` hardcoded, dessincronizado de `SESSION_TTL_HOURS`**
`query_build/__init__.py:84` e `query_analyzer/__init__.py:46` fixam `_THREAD_TTL = 3600` (1h) — sem ler `SESSION_TTL_HOURS` (padrão 8h, configurável via painel admin). Se um admin aumentar `SESSION_TTL_HOURS` esperando que sessões HITL durem mais, esses dois agentes continuam expirando em 1h de qualquer forma.

**2.6 — Nenhum agente faz streaming real de tokens**
Finance Auditor: `graph.stream(...)` é consumido internamente até o fim e devolvido como um único JSON (`__init__.py:77-78`); o efeito de "digitação" no frontend (`_faRevealText`, `scripts.js:5296-5349`) é sintético, sobre uma resposta já pronta. query_build e query_analyzer nem têm esse efeito — mostram um spinner de tempo decorrido e renderizam tudo de uma vez. Para um produto de "voz financeira" conversacional, isso é abaixo do padrão de mercado atual para chat com LLM.

**2.7 — Constantes mágicas espalhadas fora de `config.py`**
Lista não-exaustiva: `query_build/nodes.py:319` (timeout de dry-run, 20s), `query_build/graph.py:40` (repair cap = 1), `query_build/nodes.py:548` (quality retry cap = 2), `schema_graph/nodes.py:56-57` (`_ENRICH_BATCH_SIZE=30`, `_MAX_WORKERS=4`), `agents.py:152` (`_QA_RATE_LIMIT_WINDOW=60`), `agents.py:335,563` (janela de contexto de conversa, tamanho do histórico). Por outro lado, vários knobs corretos já são config-driven (`QUERY_BUILD_BUDGET_BYTES`, `QA_MAX_ITERATIONS`, `BQ_COST_PER_TB_USD` etc.) — o problema não é ausência total de padrão, é **inconsistência sobre quais constantes recebem tratamento de config**.

### ✅ Resolvido desde a auditoria de 2026-06-28
- **Cache de schema/catálogo no query_analyzer**: estava pendente, agora resolvido — `_CATALOG_CACHE`/`_SCHEMA_CACHE` com TTL de 300s (`query_analyzer/nodes.py:37-57`).
- **max_iterations do query_analyzer**: confirmado consistente entre `state.py`/`graph.py` (já havia sido resolvido).
- **Credenciais recriadas a cada nó do Schema Explorer**: resolvido, `lru_cache` funcionando em `bigquery.py._get_client()`.

### ❌ Ainda aberto (reconfirmado hoje)
- Cache de schema no `schema_graph` propriamente dito (só existe cache de resultado final, sem TTL, na camada de API).
- `max_columns` 15 vs 50 (e agora 4 valores diferentes).
- `_THREAD_TTL` dessincronizado de `SESSION_TTL_HOURS`.
- Paralelização de `document_build` e `schema_graph`.

---

## 3. Assertividade (qualidade e confiabilidade das respostas)

### Pontos fortes a reconhecer
- **Loop planner → reflect → composer** bem desenhado: `reflect` critica o plano executado e pode adicionar passos de correção, limitado a `_MAX_ITERATIONS=2` (`supervisor.py:65`) e `_MAX_PLAN_STEPS=6` (`:64`) — nunca degenera em loop infinito, mas ainda dá uma segunda chance real.
- **Fuzzy-matching para alucinação de nomes**: `_fuzzy_pick_dataset`/`_fuzzy_pick_column` (`capabilities.py:144-200`) recuperam dataset/coluna quando o LLM erra o nome exato — reduz respostas "não encontrei" desnecessárias.
- **`_normalize_plan_steps`** (`supervisor.py:233-268`) corrige preventivamente um erro comum do planner (chamar `metric_execute` sem `key`) antes mesmo de executar.
- **Retry com feedback de erro anexado**: quando `text_to_sql` falha, o erro/SQL tentado é anexado de volta ao prompt via `_attach_retry_feedback` (`supervisor.py:585-616`) para a próxima tentativa — não é um retry cego.
- **Erro de capability nunca vira exceção não tratada**: `execute_capability` (`capabilities.py:1698-1704`) retorna erro estruturado mesmo se o planner alucinar um nome de capability inexistente.

### 🟠 Achado de risco

**3.1 — Ausência de human-in-the-loop antes de executar SQL gerado por LLM**
Diferente de `query_build`/`query_analyzer` (que compilam o grafo com `checkpointer=` e expõem `POST /api/agents/{agent}/resume` com decisão humana `approve`/`skip`, `agents.py:1017-1074`), o Finance Auditor **não tem nenhum ponto de pausa** — o SQL gerado por `text_to_sql` é validado só por regras automáticas (`assert_select_only` + RBAC + budget de bytes), nunca por um humano, antes de rodar contra o BigQuery de produção. Para um agente que lida com dados financeiros, isso é uma lacuna de controle, mesmo com os guards automáticos funcionando bem.

**3.2 — Guard temporal é só instrução de prompt, não verificação de código**
`get_planner_date_block`/`get_date_block` (`src/shared/guardrails/temporal.py`) são texto injetado nos prompts do Planner/Reflect/Composer (`supervisor.py:278-280,532-534,714-717`) — não há checagem de código de que o LLM realmente usou a data fornecida em vez de uma data alucinada/desatualizada do seu treinamento.

**3.3 — Args do plano são `dict[str, Any]` livre, sem schema validado antes da execução**
`PlanStep.args` (`supervisor_schemas.py:47`) não tem um schema por capability — cada `cap_*` valida seus próprios args defensivamente em runtime. Funciona (erros viram `_err(...)` estruturado), mas significa que não há uma camada central que garanta "os args batem com o que a capability espera" antes de sequer tentar executar.

---

## 4. Boas práticas de mercado (LangGraph)

### ✅ Acertos
- **Guarda de max-iterations correta** — evita exatamente o anti-padrão "loop infinito sem saída" que a própria documentação de LangGraph adverte. `_reflect_router` sempre converge para `"composer"`.
- **Uso de state para fluxo de dados** — nenhum nó depende de estado externo escondido; tudo passa pelo `SupervisorState`, seguindo o padrão recomendado (evita o anti-padrão "nós sem estado").

### 🟠 Divergências da idiomática LangGraph (dívida arquitetural)

**4.1 — Grafo compilado sem checkpointer nativo**
`build_supervisor_graph(...).compile()` (`supervisor.py:856`) não recebe `checkpointer=`, ao contrário de `query_analyzer/graph.py:102` e `query_build/graph.py:129`. Persistência é reimplementada à mão (`FileCheckpointer`, JSON por chave em disco) só na camada de API, salvando a *resposta final* e o *histórico de chat* — não um snapshot do estado do grafo. Consequência prática: **não há resume real de execução parcial** (se o processo cair no meio de um plano de 6 passos, tudo se perde) e **não há suporte a interrupt/HITL nativo** (item 3.1). Os agentes irmãos já resolveram isso corretamente — vale trazer o Finance Auditor para o mesmo padrão.

**4.2 — Capabilities não são tools LangChain (`bind_tools`/`ToolNode`)**
`CAPABILITY_REGISTRY` (`capabilities.py:1679-1695`) é um dicionário nome→função, dirigido por um plano gerado via structured output — não pelo mecanismo nativo de tool-calling do LangChain (o LLM nunca vê um schema JSON de tool, só uma descrição em texto livre no prompt, `supervisor_prompts.py:15-273`). Essa é uma escolha de arquitetura deliberada (plan-and-execute com DAG e paralelismo interno via `ThreadPoolExecutor`, em vez de ReAct passo-a-passo) e **não é "errada"** — mas diverge do padrão que o próprio ecossistema LangGraph documenta como recomendado, perdendo validação automática de schema de tool-call e compatibilidade com tooling que espera `tool_calls` nativos (ex.: tracing de tool-use no LangSmith fica menos estruturado).

**4.3 — Estado 100% monolítico, sem reducers**
`SupervisorState` (`supervisor_state.py:8-52`) é um único `TypedDict(total=False)` plano, sem nenhum `Annotated[..., reducer]` — nem `add_messages`, nem reducer customizado. Hoje isso não causa bug porque a execução é single-threaded por invocação (o paralelismo do `node_router` é interno via `ThreadPoolExecutor`, não paralelismo de nós do grafo). Mas é o anti-padrão "estado gigante monolítico" citado pela própria skill de LangGraph — se o grafo algum dia ganhar branches paralelos de verdade, campos tipo `tool_results`/`warnings`/`plan` (hoje sobrescritos inteiros a cada nó) vão colidir silenciosamente sem um reducer.

---

## 5. Lista priorizada de correções

Ordenada por impacto × esforço. Itens marcados `[06-28]` já estavam na auditoria anterior e continuam abertos — não são achados novos, só reconfirmados.

| # | Item | Dimensão | Esforço | Por quê primeiro/depois |
|---|---|---|---|---|
| 1 | Adicionar HITL (approve/skip) antes de rodar SQL gerado por LLM no Finance Auditor, reusando o padrão já existente em query_build/query_analyzer (3.1, 4.1) | Assertividade / Arquitetura | Alto | Maior mudança estrutural da lista, mas fecha a maior lacuna de controle humano |
| 2 | Compilar o grafo do Finance Auditor com `checkpointer=` nativo do LangGraph (4.1) | Boas práticas | Alto | Pré-requisito técnico para o item 1; também habilita resume real de execução parcial |
| 3 | Adicionar `invoke_with_retry` na chamada LLM do `schema_graph` (2.1) | Produtividade | Baixo | Trivial, alinha o único agente que ainda falta |
| 4 | Paralelizar `enrich_with_llm` no `schema_graph` com `ThreadPoolExecutor` (já existe no mesmo arquivo) (2.2) `[06-28]` | Produtividade | Baixo-Médio | Padrão já existe no arquivo, só replicar |
| 5 | Paralelizar `fetch_real_schema`/`fetch_dataplex_tags` no `document_build` (2.3) `[06-28]` | Produtividade | Baixo-Médio | Mesma ideia, sem dependência de dados real entre eles |
| 6 | Unificar `max_columns` entre `get_dataset_tables_metadata`/`get_dataset_tables_schema` e reduzir os 4 valores divergentes a 1-2 justificados (2.4) `[06-28]` | Produtividade | Baixo | Consolidação simples, mas crescendo em divergência a cada sprint que não é tratada |
| 7 | Sincronizar `_THREAD_TTL` com `SESSION_TTL_HOURS` via `get_runtime_config` (2.5) `[06-28]` | Produtividade | Baixo | Config já existe, só falta ler dela |
| 8 | Avaliar streaming real (token-a-token) para Finance Auditor, ou pelo menos estender o efeito de digitação sintético para query_build/query_analyzer (2.6) | Produtividade / UX | Médio-Alto | Maior esforço de UX da lista; não é bug, é expectativa de mercado para chat com LLM |
| 9 | Migrar capabilities para `bind_tools`/`ToolNode` nativo, ou documentar deliberadamente por que o dispatcher próprio foi escolhido (4.2) | Boas práticas | Alto | Mudança arquitetural grande; só vale se os itens 1-2 (HITL + checkpointer) forem adiante primeiro, já que ambos se beneficiam de uma reestruturação conjunta |
| 10 | Adicionar reducers (`Annotated[...]`) nos campos de lista do `SupervisorState` mesmo sem paralelismo de nós hoje, como blindagem futura (4.3) | Boas práticas | Baixo-Médio | Baixo risco imediato, mas barato de corrigir agora vs. caro de depurar depois |
| 11 | Consolidar constantes mágicas restantes em `config.py` (2.7) `[06-28]` | Produtividade | Baixo (mas repetitivo) | Item de manutenção contínua, não bloqueia nada |

---

## Nota final

Este documento é atualizado conforme itens da lista são implementados: cada correção aplicada sai da análise/priorização e vira uma entrada no "Changelog de implementação" no topo do arquivo, com os arquivos alterados. A lista priorizada atual reflete só o que ainda está em aberto. A decisão de quais atacar e em que sprint continua com o usuário.
