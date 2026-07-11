# Finance Voice — cost-safety, observabilidade e teste de integração

**Escopo:** `src/agents/finance_auditor/*` — continuação da auditoria em
`finance-voice.md`, focada em achados de custo/performance/LangGraph que
ficaram fora daquele documento (streaming real permanece rastreado só em
`finance-voice.md`, não é repetido aqui).
**Método:** leitura direta do código atual, verificada linha a linha antes de
cada mudança (assinaturas de função, variáveis de budget, mensagens de erro
reais). Toda afirmação abaixo tem citação `arquivo:linha`.

## Changelog de implementação

Atualizado a cada item resolvido. Serve como checklist para portar
manualmente para o GitLab da empresa (repo separado, sem remote/histórico
compartilhado com este).

| Arquivo | Funções |
|---|---|
| `src/shared/tools/bigquery.py` | `execute_query_rows` (parâmetro `maximum_bytes_billed` 🆕) |
| `src/agents/finance_auditor/capabilities.py` | `_validate_and_run_sql` (cap duro + mensagem de erro dedicada), `_get_cached_catalog_search`, `cap_attachment_analyze` (`usage_sink`/`run_config` 🆕) |
| `src/agents/finance_auditor/agentic_retrieval.py` | `transform_query`, `adaptive_search_catalog` (`usage_sink`/`run_config` 🆕, roteamento por `invoke_with_retry`) |
| `src/agents/finance_auditor/multimodal.py` | `describe_image_with_llm` (`usage_sink`/`run_config` 🆕, roteamento por `invoke_with_retry`) |
| `src/shared/tools/llm.py` | `invoke_with_retry`, `invoke_with_retry_async` (parâmetro `run_config` 🆕) |
| `src/agents/finance_auditor/supervisor.py` | `_trace_config` 🆕, `node_planner`, `node_reflect`, `node_composer` (passam `run_config`), `build_supervisor_graph` (`compile(name=...)`) |
| `tests/agents/test_finance_auditor_supervisor_integration.py` | novo arquivo 🆕 — teste de integração do grafo compilado |

🆕 = função/arquivo novo (as demais são funções existentes que foram alteradas).

**Portar para o GitLab:** repositório separado sem remote compartilhado —
replicar manualmente usando a tabela acima, ou pedir um `git diff` dos
arquivos para aplicar com `git apply` se a base for igual.

---

## Resumo executivo

| Dimensão | Status |
|---|---|
| **Cost-safety (LLM)** | ✅ Resolvido — bypass de budget/observabilidade fechado em duas chamadas diretas (ver Changelog). |
| **Cost-safety (BigQuery)** | ✅ Resolvido — cap duro de `maximum_bytes_billed` na execução real, além da estimativa de dry-run já existente. |
| **Observabilidade** | ✅ Resolvido — `run_name`/`tags`/`metadata` propagados a todas as chamadas LLM do grafo e ao `compile()`, no-op seguro quando LangSmith está desligado. |
| **Testes** | ✅ Resolvido — primeiro teste de integração do `StateGraph` real (compile+stream), cobrindo roteamento condicional e checkpointer de verdade. |

Streaming real de tokens (achado 2.6 de `finance-voice.md`) e reducers de
estado (achado 4.1 de `finance-voice.md`) permanecem em aberto naquele
documento — não fazem parte desta rodada.

---

## 1. Cost-safety — bypass de budget/observabilidade

Duas chamadas LLM diretas no código furavam o circuit-breaker de custo
(`TokenBudgetExceeded`) e ficavam invisíveis no `usage_log`/audit — o
orçamento de tokens configurado (`FINANCE_AUDITOR_TOKEN_BUDGET`) não
protegia esses dois caminhos:

- `transform_query` (`agentic_retrieval.py`, reescrita de query do RAG
  quando o grade do `catalog_search` vem abaixo de `_GRADE_THRESHOLD_DEFAULT
  = 0.65`) chamava `llm.invoke(...)` direto.
- `describe_image_with_llm` (`multimodal.py`, análise de imagem anexada)
  chamava `llm.invoke([message])` direto.

Ambas passaram a rotear por `invoke_with_retry` (com `usage_sink`/`label`
próprios: `agentic_retrieval_transform_query` e `attachment_analyze_image`),
herdando retry automático, aparecendo em `token_usage.by_node`, e sobretudo
ficando sujeitas ao circuit-breaker de orçamento. Os callers
(`_get_cached_catalog_search`, `cap_attachment_analyze`) passaram a
propagar `context.get("usage_log")` — nenhuma mudança de shape de `context`
foi necessária, a chave já existia.

**Trade-off aceito:** as duas funções ganharam retry (2 tentativas) onde
antes era uma tentativa única — latência de pior caso sobe um pouco, só em
caminhos já excepcionais (grade de RAG baixo, ou análise de imagem). E
passam a poder falhar com `TokenBudgetExceeded` se o budget da requisição já
tiver sido gasto em steps anteriores do plano — comportamento correto e
esperado, mas é uma mudança observável (nova mensagem de erro possível em
planos longos).

---

## 2. Cost-safety — cap duro de custo no BigQuery

`execute_query_rows` (`bigquery.py`) executava a query real sem
`maximum_bytes_billed` no `QueryJobConfig` — só existia uma estimativa
prévia de dry-run (`_validate_and_run_sql`, contra
`FINANCE_AUDITOR_QUERY_BUDGET_BYTES`). Se o volume real mudasse entre a
estimativa e a execução (ex.: tabela particionada com carga concorrente),
nada impedia estourar o orçamento configurado — função irmã
`fetch_query_sample` já tinha esse freio, `execute_query_rows` não.

`execute_query_rows` ganhou o parâmetro opcional `maximum_bytes_billed`
(default `None`, preserva comportamento atual para o caller de sync interno
do catálogo). `_validate_and_run_sql` passou a reaproveitar a mesma variável
`budget` já lida do dry-run — mesma régua para estimativa e execução real.
Quando o BigQuery cancela por estourar o cap, a mensagem de erro ao usuário
agora orienta "refine os filtros" em vez de expor o erro cru da API.

**Trade-off aceito:** uma query que passou no dry-run mas cresce entre a
estimativa e a execução real agora falha explicitamente em vez de rodar e
cobrar mais — é o objetivo do item, mas pode gerar mensagens de erro novas
em datasets voláteis.

---

## 3. Observabilidade — LangSmith (tags/run_name/metadata)

Nenhuma chamada LLM do Finance Auditor carregava `run_name`/`tags` — quando
o tracing está ativo (`configure_tracing`, condicional a `LANGCHAIN_API_KEY`
configurado), ficava difícil filtrar traces por agente/nó/persona sem
heurística manual pelo nome genérico do nó.

`invoke_with_retry`/`invoke_with_retry_async` ganharam parâmetro opcional
`run_config`, repassado a `llm.invoke(messages, config=run_config)`. Um
helper novo, `_trace_config(label, state)`, centraliza a montagem do dict
(`run_name=f"finance_auditor:{label}"`, `tags=["finance_auditor", label]`,
`metadata={"project_id", "persona"}`) para os três nós que chamam LLM
(planner/reflect/composer), evitando duplicar a construção em cada um. Os
dois caminhos do item 1 (catalog_search rewrite, attachment analyze) também
passaram a carregar tags equivalentes. `build_supervisor_graph(...).compile()`
passou a receber `name="finance_auditor_supervisor"`.

**Por que é seguro com LangSmith desligado:** `config=` em
`Runnable.invoke`/`ainvoke` é sempre consumido pelo `RunnableConfig` do
LangChain core — sem callback handler de tracing registrado, os campos são
apenas ignorados, sem mudança de comportamento nem erro.

---

## 4. Testes — integração do grafo compilado

Toda a suíte existente (`tests/agents/test_finance_auditor_supervisor.py`)
mockava `build_supervisor_graph` inteiro ou chamava funções de nó isoladas
com `MagicMock()` como LLM — nenhum teste subia o `StateGraph` real
(`compile()` + `.stream()`), exercitando roteamento condicional real
(`_reflect_router`) e um checkpointer de verdade.

Novo arquivo `tests/agents/test_finance_auditor_supervisor_integration.py`,
com dois testes:
- **Ciclo completo com `chat_answer`** — plano de sucesso faz `node_reflect`
  early-return sem chamar LLM (capability já está em `_ANSWER_PRODUCING`),
  então só planner/composer precisam de mock. Usa `MemorySaver()` real (não
  mock) como checkpointer, e `audit_log.record` é mockado só para não gravar
  no SQLite de dev durante o teste.
- **Guardrail de entrada bloqueando** — prova que `node_planner` lê
  `guardrail_in_ok` e pula a chamada ao LLM (retorna plano vazio) quando o
  guardrail de injeção rejeita a entrada, sem precisar de edge condicional
  dedicado no grafo para isso.

Ambos passam. Suíte completa do finance_auditor rodada após as mudanças dos
itens 1-3: **183 passaram, 1 falha pré-existente e não relacionada**
(`test_gera_artefato_de_audio`, tabela `finance_podcast_assets` ausente no
`.sixth/app.db` local — confirmado via `git stash` que já falhava antes
destas mudanças; é uma lacuna de migração do banco de dev, não uma
regressão introduzida aqui).

---

## Nota final

Este documento é atualizado conforme itens são implementados: cada correção
aplicada sai da análise e vira uma entrada no "Changelog de implementação"
no topo do arquivo, com os arquivos alterados. Itens já implementados ou com
decisão final tomada (fix ou não-fix) saem do corpo do relatório. Streaming
real de tokens e reducers de estado continuam rastreados em
`finance-voice.md`, não neste documento.
