# Avaliação profunda — Finance Voice (Finance Auditor)

**Data:** 2026-07-09
**Escopo:** `src/agents/finance_auditor/*` — só o agente Finance Voice/Finance Auditor. Outros agentes (`query_build`, `query_analyzer`, `document_build`, `schema_graph`) ficam fora deste documento.
**Dimensões avaliadas:** Segurança · Produtividade/Performance (incluindo eficiência de tokens/custo de LLM) · Assertividade · Boas práticas de mercado (LangGraph).
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
| 2026-07-09 | `src/agents/finance_auditor/supervisor.py` | Removida a cópia hardcoded de `_INJECTION_MARKERS`; `node_guardrails_in` agora chama `injection.check_injection()` compartilhado | 1.4 |
| 2026-07-09 | `src/api/routes/agents.py` | `GET /api/runtime-llm` e `GET /api/agents` agora exigem `Depends(get_current_user)` | 1.6 |
| 2026-07-09 | `README.md` | Lista de endpoints atualizada: `/api/runtime-llm` movido de "Públicos" para "Protegidos por sessão" | 1.6 |
| 2026-07-09 | `src/agents/finance_auditor/supervisor.py` | `_resolve_placeholders` (late-binding `${step_N.path}`) só resolve valores escalares — dict/list resolvido fica sem substituir em vez de virar `str({...})` cru no arg; resultado limitado a 4000 chars | 1.7 |

**Portar para o GitLab:** repositório separado sem remote compartilhado — replicar manualmente usando a tabela acima, ou pedir um `git diff` dos arquivos para aplicar com `git apply` se a base for igual.

---

## Resumo executivo

| Dimensão | Status |
|---|---|
| **Segurança** | ✅ Nenhum achado em aberto — tudo corrigido nesta auditoria (ver Changelog). |
| **Produtividade/Performance** | ⚠️ 9 achados abertos (seção 2), todos de eficiência de tokens/custo de LLM. Maiores: prompt do planner sem cache de contexto (~3,7k tokens estáticos reenviados em toda chamada) e ausência de streaming real de tokens. |
| **Assertividade** | ⚠️ 3 achados abertos (seção 3). Principal: sem human-in-the-loop antes de rodar SQL gerado por LLM. |
| **Boas práticas LangGraph** | ⚠️ 3 achados abertos (seção 4). Principais: grafo sem checkpointer nativo (perde resume/interrupt) e capabilities fora do padrão `bind_tools`/`ToolNode`. |

---

## 1. Segurança

Nenhum achado em aberto nesta rodada — todos implementados, ver Changelog no topo do arquivo.

### 🟢 Verificado como não-problema (para não reabrir como "achado novo")
- `assert_select_only` (SQL guard) está corretamente integrado em todos os pontos que executam SQL gerado por LLM no Finance Auditor (`capabilities.py:400`, `alerting.py:24,101`). Único ponto sem o guard é `catalog_index.py:184`, mas ali o SQL é um template fixo (`SELECT * FROM {table_ref}`) com `table_ref` vindo do catálogo real do BigQuery, não de texto do usuário/LLM — não é injetável.
- Sessão: senha com bcrypt em 100% dos caminhos de escrita; token de sessão é opaco (UUID) e validado contra a base a cada request — não há como forjar/escalar via manipulação de token.
- Admin bootstrap gera senha aleatória forte se `ADMIN_DEFAULT_PASSWORD` não for setado — não há credencial fraca hardcoded.
- CORS: `allow_credentials=False`, auth via Bearer token (não cookie) — configuração de `ALLOWED_ORIGINS` aberta não implica roubo de sessão.
- `.sixth/app.db` não é servido por nenhuma rota web — acesso é só a nível de filesystem.

---

## 2. Produtividade / Performance

### 🟡 Médio

**2.6 — Sem streaming real de tokens**
`graph.stream(...)` é consumido internamente até o fim e devolvido como um único JSON (`__init__.py:77-78`); o efeito de "digitação" no frontend (`_faRevealText`, `scripts.js:5296-5349`) é sintético, sobre uma resposta já pronta. Para um produto de "voz financeira" conversacional, isso é abaixo do padrão de mercado atual para chat com LLM.

### 🟠 Eficiência de tokens / custo de LLM (achados novos desta rodada)

**2.7 — Prompt do planner sem cache de contexto: ~3,7k tokens estáticos reenviados em toda chamada**
`PLANNER_PROMPT` (`supervisor_prompts.py:7-281`) tem 14.653 caracteres (~3.700 tokens) — embute o catálogo completo das 14 capabilities com args e exemplos, mesmo quando o plano final usa só 1-2 delas. Esse bloco é praticamente idêntico entre chamadas (só a data muda, `supervisor.py:283-285`), mas é reenviado e retokenizado do zero em toda invocação do planner, inclusive para follow-ups triviais ("obrigado", "e no mês passado?"). Não há uso de Vertex AI Context Caching em lugar nenhum do projeto (`grep` por `cached_content`/`context_cach`/`CachedContent`/`cache_control`/`prompt_caching` em `src/` não encontra nada). Essa é a maior alavanca de economia disponível: um turno típico já gasta ~4,5k tokens só de entrada no planner.

**2.8 — Contexto de schema recalculado e duplicado entre capabilities e retries**
`cap_text_to_sql` busca schema via `get_table_schema()` sem cache (`capabilities.py:742-747`) — chamada viva ao BigQuery toda vez. Se o Planner também agenda um step `bq_get_schema` explícito pro mesmo `table_ref` (padrão documentado no próprio prompt, `supervisor_prompts.py:26-28`), o schema é buscado e serializado no contexto do LLM **duas vezes** no mesmo turno. Em um retry disparado pelo Reflect, `text_to_sql` reconstrói o schema do zero em vez de reaproveitar o que já foi buscado na tentativa anterior (`_attach_retry_feedback`, `supervisor.py:590-621`, só reanexa o SQL/erro anterior, não o schema já obtido).

**2.9 — Chamadas LLM auxiliares descartadas sem cache, uma delas fora do padrão de retry**
`_pick_relevant_tables` (`capabilities.py:544-606`) é uma chamada LLM separada — até 80 tabelas × 20 colunas em JSON — cujo resultado nunca é cacheado/reaproveitado dentro do turno. `cap_catalog_search` e o branch RAG de `cap_text_to_sql` chamam `adaptive_search_catalog()` de forma independente; se o Planner agendar `catalog_search` E `text_to_sql` sem `dataset_ref` (algo que o próprio prompt desaconselha mas não impede, `supervisor_prompts.py:265-270`), a mesma busca por embedding roda duas vezes. `transform_query` (`agentic_retrieval.py:62`) faz uma chamada `llm.invoke()` direta, sem passar por `invoke_with_retry` — fora do padrão de retry/observabilidade do resto do agente.

**2.10 — `conversation_context` montado uma vez, mas pago duas vezes**
A mesma string (~400-600 tokens, 2 turnos anteriores truncados a 800 chars cada, `agents.py:335-355,558`) é embutida separadamente no prompt do planner (`supervisor.py:290-299`) e do composer (`supervisor.py:727-736`) — como são duas chamadas de API distintas, o mesmo texto é pago (tokenizado e cobrado) duas vezes por turno.

**2.11 — Um único modelo para tudo, sem tiering por complexidade**
`gemini-2.5-flash` (`llm.py:36-63`) é usado sem variação para planner, reflect, composer, `text_to_sql`, `_pick_relevant_tables` e descrição de imagem — só a temperatura varia (0.05 analítico vs 0.3 criativo pro composer). Tarefas simples de classificação (`_pick_relevant_tables`, o veredito curto do Reflect) rodam no mesmo modelo que gera SQL ou o relatório final, sem opção de usar algo mais barato/rápido onde a tarefa é trivial.

**2.12 — Token usage capturado mas nunca persistido — sem visibilidade de custo real**
`get_usage_metadata_callback` (`finance_auditor/__init__.py:76-137`) captura de fato `input_tokens`/`output_tokens`/`total_tokens` reais de cada chamada LLM do turno e devolve o total agregado pro frontend (`scripts.js:6203`). Mas isso nunca é: (a) quebrado por nó (planner vs. composer vs. text_to_sql não são distinguíveis no agregado), nem (b) persistido em lugar nenhum — `audit.record()` (`audit.py:36-58`) só grava bytes/custo de **BigQuery**, nunca tokens de LLM. Hoje é impossível olhar o histórico e responder "o que está consumindo mais tokens em produção".

**2.13 — Sem budget/circuit-breaker de tokens por requisição**
BigQuery tem `FINANCE_AUDITOR_QUERY_BUDGET_BYTES` como teto (`capabilities.py:393-455`), mas não existe nada equivalente para tokens/custo de LLM. Um turno worst-case (plano de 6 passos, modo análise profunda, um retry do Reflect que dobra o volume de `tool_results` no composer) pode passar de 20-30k tokens de entrada sem nenhum limite, alerta ou corte. Isso é ao mesmo tempo um risco de custo e uma superfície de abuso — nada impede um usuário de repetir deliberadamente o pior caso.

**2.14 — Retry reenvia o prompt inteiro, sem redução**
`invoke_with_retry` (`llm.py:66-100`) reenvia a mensagem completa e idêntica a cada tentativa — nenhuma redução de contexto entre tentativas. Uma falha transitória em `text_to_sql` pode custar até 3 chamadas completas (`max_attempts=2` + 1 fallback, `capabilities.py:766-787`), cada uma carregando o `schemas_text` inteiro.

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
O Finance Auditor **não tem nenhum ponto de pausa** — o SQL gerado por `text_to_sql` é validado só por regras automáticas (`assert_select_only` + RBAC + budget de bytes), nunca por um humano, antes de rodar contra o BigQuery de produção. Para um agente que lida com dados financeiros, isso é uma lacuna de controle, mesmo com os guards automáticos funcionando bem.

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
`build_supervisor_graph(...).compile()` (`supervisor.py:856`) não recebe `checkpointer=`. Persistência é reimplementada à mão (`FileCheckpointer`, JSON por chave em disco) só na camada de API, salvando a *resposta final* e o *histórico de chat* — não um snapshot do estado do grafo. Consequência prática: **não há resume real de execução parcial** (se o processo cair no meio de um plano de 6 passos, tudo se perde) e **não há suporte a interrupt/HITL nativo** (item 3.1).

**4.2 — Capabilities não são tools LangChain (`bind_tools`/`ToolNode`)**
`CAPABILITY_REGISTRY` (`capabilities.py:1679-1695`) é um dicionário nome→função, dirigido por um plano gerado via structured output — não pelo mecanismo nativo de tool-calling do LangChain (o LLM nunca vê um schema JSON de tool, só uma descrição em texto livre no prompt, `supervisor_prompts.py:15-273`). Essa é uma escolha de arquitetura deliberada (plan-and-execute com DAG e paralelismo interno via `ThreadPoolExecutor`, em vez de ReAct passo-a-passo) e **não é "errada"** — mas diverge do padrão que o próprio ecossistema LangGraph documenta como recomendado, perdendo validação automática de schema de tool-call e compatibilidade com tooling que espera `tool_calls` nativos (ex.: tracing de tool-use no LangSmith fica menos estruturado).

**4.3 — Estado 100% monolítico, sem reducers**
`SupervisorState` (`supervisor_state.py:8-52`) é um único `TypedDict(total=False)` plano, sem nenhum `Annotated[..., reducer]` — nem `add_messages`, nem reducer customizado. Hoje isso não causa bug porque a execução é single-threaded por invocação (o paralelismo do `node_router` é interno via `ThreadPoolExecutor`, não paralelismo de nós do grafo). Mas é o anti-padrão "estado gigante monolítico" citado pela própria skill de LangGraph — se o grafo algum dia ganhar branches paralelos de verdade, campos tipo `tool_results`/`warnings`/`plan` (hoje sobrescritos inteiros a cada nó) vão colidir silenciosamente sem um reducer.

---

## 5. Lista priorizada de correções

Ordenada por uma lógica de **medir → cachear → limitar → deduplicar → tiering → arquitetura**: primeiro os itens que dão visibilidade de custo e fecham o maior ralo de tokens (baratos, altíssimo retorno), depois os itens estruturais que já estavam na lista anterior.

| # | Item | Dimensão | Esforço | Por quê primeiro/depois |
|---|---|---|---|---|
| 1 | Persistir `token_usage` no audit log com breakdown por nó (planner/reflect/composer/capabilities) (2.12) | Produtividade / Custo | Baixo | Pré-requisito barato pra medir qualquer otimização seguinte — sem isso, tudo abaixo é chute |
| 2 | Ativar Vertex AI Context Caching no prompt do planner (2.7) | Produtividade / Custo | Médio | Maior alavanca de economia isolada — ~3,7k tokens estáticos reenviados em toda chamada |
| 3 | Adicionar budget/circuit-breaker de tokens por requisição, espelhando o `FINANCE_AUDITOR_QUERY_BUDGET_BYTES` do BigQuery (2.13) | Segurança / Custo | Médio | Fecha risco de custo descontrolado e superfície de abuso |
| 4 | Eliminar redundância de contexto: schema duplicado (`bq_get_schema` + `text_to_sql`), `conversation_context` duplicado (planner + composer), cache de `_pick_relevant_tables`/`catalog_search` dentro do turno (2.8, 2.9, 2.10) | Produtividade / Custo | Médio | Vários ganhos pequenos e independentes, seguros de implementar juntos |
| 5 | Reduzir amplificação de custo em retry — reaproveitar contexto já buscado (schema, tabelas escolhidas) em vez de reconstruir do zero a cada tentativa (2.14) | Produtividade / Custo | Baixo-Médio | Resolve naturalmente junto do item 4 |
| 6 | Model tiering para chamadas auxiliares simples (`_pick_relevant_tables`, veredito do Reflect) — usar um modelo mais barato/rápido (2.11) | Produtividade / Custo | Médio | Só compensa com dado real do item 1 mostrando o quanto essas chamadas pesam hoje |
| 7 | Adicionar HITL (approve/skip) antes de rodar SQL gerado por LLM no Finance Auditor (3.1, 4.1) | Assertividade / Arquitetura | Alto | Maior mudança estrutural da lista, mas fecha a maior lacuna de controle humano |
| 8 | Compilar o grafo do Finance Auditor com `checkpointer=` nativo do LangGraph (4.1) | Boas práticas | Alto | Pré-requisito técnico para o item 7; também habilita resume real de execução parcial |
| 9 | Avaliar streaming real (token-a-token) para o Finance Auditor (2.6) | Produtividade / UX | Médio-Alto | Não é bug, é expectativa de mercado para chat com LLM |
| 10 | Migrar capabilities para `bind_tools`/`ToolNode` nativo, ou documentar deliberadamente por que o dispatcher próprio foi escolhido (4.2) | Boas práticas | Alto | Mudança arquitetural grande; só vale se os itens 7-8 (HITL + checkpointer) forem adiante primeiro |
| 11 | Adicionar reducers (`Annotated[...]`) nos campos de lista do `SupervisorState` mesmo sem paralelismo de nós hoje, como blindagem futura (4.3) | Boas práticas | Baixo-Médio | Baixo risco imediato, mas barato de corrigir agora vs. caro de depurar depois |

---

## Nota final

Este documento é atualizado conforme itens da lista são implementados: cada correção aplicada sai da análise/priorização e vira uma entrada no "Changelog de implementação" no topo do arquivo, com os arquivos alterados. A lista priorizada atual reflete só o que ainda está em aberto. Nenhuma implementação foi feita nesta rodada — é só análise e priorização, incluindo a nova dimensão de eficiência de tokens/custo. A decisão de quais atacar e em que sprint continua com o usuário.
