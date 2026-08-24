# Query Transformer — design

## Contexto

O finance-hub tem hoje 5 agentes (Query Builder, SQL Review, Document Builder, Schema Explorer, Finance Voice IA), cada um implementando `BaseAgent` (`src/core/base_agent.py`) como um wrapper fino sobre um grafo LangGraph compilado. Este documento desenha um sexto agente: **Query Transformer**, que converte SQL do BigQuery em modelos `.sqlx` do Dataform, seguindo boas práticas de mercado (particionamento, tipo de materialização, ausência de `SELECT *`, etc.), sem alterar o resultado final da query, e com HITL (human-in-the-loop) + RAG.

Decisões já fechadas com o usuário:
- **Nome**: "Query Transformer" (ao lado de "Query Builder" e "SQL Review" no menu, mas claramente distinto na função).
- **Entrada**: standalone — o usuário cola a SQL do BigQuery diretamente num card novo na tela inicial, sem hand-off automático de outro agente.
- **Escopo Dataform**: texto autônomo. O agente **não** se conecta a um repositório Dataform real — gera o `.sqlx` como texto pronto, com `ref()` sugerido com base nos nomes de tabela que a própria SQL referencia, sem resolver contra um grafo de dependências real.
- **RAG**: base de conhecimento curada de boas práticas Dataform/SQLX (criada do zero), não schema do BigQuery.
- **HITL**: só pausa para aprovação humana quando o score de qualidade fica baixo **ou** a validação de equivalência falha — nunca em toda execução, nunca nenhuma vez (padrão idêntico ao Query Builder).
- **Validação de equivalência**: dry-run comparando schema de colunas + custo/bytes entre a SQL original e a query compilada do SQLX gerado — sem executar de verdade (sem custo real de BigQuery).
- **Tipo de materialização**: decidido automaticamente pelo agente (table/view/incremental), com justificativa no relatório final.

## Posicionamento

- `agent_id`: `query_transformer`
- Implementa `BaseAgent`, registrado em `src/core/registry.py` via `src/api/dependencies.py` (mesmo padrão dos outros 5).
- Entrada no catálogo de metadados (`src/core/agent_catalog.py`): display_name "Query Transformer", ícone novo (`icon_token: "swap"` — setas bidirecionais, representando a transformação SQL→SQLX), e um **token de cor novo** (`color_token: "indigo"` ou similar) — os 5 tokens de cor existentes (porto/teal/violet/emerald/amber) já estão todos ocupados por um agente cada (regra de "cada agente tem cor única" estabelecida nesta mesma sessão), então este agente precisa de uma 6ª cor distinta: adicionar `--indigo` (ou nome equivalente) em `style.css:root`, mais as classes `.bi-indigo`/`.bot-card[data-color="indigo"]` seguindo exatamente o padrão já usado para `amber`.
- Aparece como card normal na tela inicial, elegível para classificação por tag em "Classificar Agentes" como qualquer outro agente — nenhuma mudança no sistema de controle de acesso por tag é necessária.

## Contrato de entrada/saída

**Entrada** (`analyze()`, mesma assinatura-base dos outros agentes):
- `query: str` — a SQL do BigQuery a converter.
- `project_id: str`.
- Sem `dataset_hint` obrigatório (a SQL já referencia seus próprios datasets/tabelas totalmente qualificados).

**Saída** (`_format_result()`, mesmo espírito dos demais agentes):
- `sqlx_content: str` — o arquivo `.sqlx` pronto (bloco `config()` + query), pronto para copiar/baixar.
- `markdown_report: str` — explicação legível: tipo de materialização escolhido e por quê, otimizações aplicadas (particionamento/cluster preservado, `SELECT *` eliminado, `ref()` sugeridos), resultado da validação de equivalência (schema/custo comparados original vs. gerado), e a decisão do HITL quando aplicável.
- `artifacts: list[dict]` — pelo menos um artifact tipo `"code"` com o `sqlx_content`, para exibição com syntax highlight no frontend (reaproveita o mecanismo de artifacts já usado pelos outros agentes).
- `status: "ok" | "awaiting_approval" | "error"` — mesmo contrato dos agentes com HITL (`query_build`, `query_analyzer`).

## Arquitetura do grafo (LangGraph)

Modelado diretamente no template do Query Builder (`src/agents/query_build/graph.py`), o agente mais próximo em propósito hoje (gera SQL, valida qualidade, tem HITL nativo via `interrupt()`, guardrails de entrada/saída, audit log). Novo módulo `src/agents/query_transformer/` com `graph.py`, `nodes.py`, `__init__.py` espelhando a estrutura de `query_build/`.

Nós, em ordem:

1. **`check_access`** — valida via RBAC existente (`src/shared/guardrails/rbac.py`, `check_dataset`) que o usuário tem acesso aos datasets/tabelas referenciados na SQL colada.
2. **`guardrails_in`** — bloqueia SQL vazia, não-`SELECT` (DDL/DML — modelos Dataform são transformações de leitura) ou trivial demais (`SELECT 1`, sem FROM).
3. **`dry_run_original`** — dry-run da SQL original (reaproveita `dry_run_query()` de `src/shared/tools/bigquery.py`) → schema de colunas + custo/bytes baseline.
4. **`generate_sqlx`** — LLM com **saída estruturada** (`with_structured_output`, não texto livre, para evitar parsing frágil de markdown/regex) devolvendo um schema com `config_block`, `query_body`, `materialization_type`, `suggested_refs` e `rationale`. Prompt enriquecido com os trechos mais relevantes recuperados do RAG de boas práticas.
5. **`dry_run_generated`** — extrai a query pura do SQLX gerado (substitui `${ref(...)}` de volta pelos nomes de tabela literais, já que não há compilador Dataform real disponível) e roda dry-run.
6. **`validate_equivalence`** — comparação determinística: mesmo conjunto/tipos de colunas entre `dry_run_original` e `dry_run_generated`, e comparação de custo/bytes. Produz um resultado pass/fail com diff explicado.
7. **`score_quality`** — checks determinísticos (uso correto de `config()`, presença de `ref()` em vez de nome de tabela cru, ausência de `SELECT *`, particionamento/cluster preservados, presença de assertions básicas quando aplicável) + LLM, no mesmo espírito de `_deterministic_quality_checks()`/`score_query()` do Query Builder (`src/agents/query_build/nodes.py`).
8. **`await_quality_approval`** — gate HITL via `interrupt()` nativo do LangGraph (mesmo padrão de `query_build/nodes.py:540` e `query_analyzer/nodes.py:340`). Pausa **se** o score de qualidade ficar abaixo do limite **ou** se `validate_equivalence` falhar (equivalência falha sempre pausa, independente do score — correção do resultado nunca é negociável). Um único repair automático (regenera o SQLX com o feedback do que falhou) antes de pausar para o humano — nunca um loop sem limite (anti-padrão de autonomia ilimitada).
9. **`guardrails_out`** — sanity check final da resposta antes de devolver (sem vazar nomes internos de projeto/dataset além do que já está na query original, mesmo espírito do `_looks_like_tech_leak` do Finance Auditor).
10. **`record_audit`** — nó de fan-in, roda sempre (sucesso, repair ou rejeitado pelo humano), para rastreabilidade — mesmo padrão de `record_audit` do Query Builder.

Roteamento condicional: os mesmos helpers `_guard`/`_guard_repairable` do Query Builder, adaptados — qualquer erro em qualquer nó roteia para `record_audit` em vez de travar o grafo sem log.

## HITL (human-in-the-loop)

`interrupt()` nativo do LangGraph (não é só um dialog de frontend) — o grafo pausa de verdade, com estado persistido no checkpointer, esperando `Command(resume=human_decision)`. Payload do interrupt inclui: o SQLX gerado, o motivo da pausa (score baixo, equivalência falhou, ou ambos), e o diff de equivalência quando aplicável — para o humano decidir entre aprovar, pedir novo repair, ou rejeitar.

Resumo exposto via `resume(thread_id, human_decision)` no `QueryTransformerAgent`, espelhando `query_build/__init__.py:177-215`.

## Memória / checkpointer

Um `MemorySaver()` (checkpointer nativo do LangGraph, em memória) próprio deste agente, instanciado como singleton dentro de `src/agents/query_transformer/__init__.py` — **não** o `FileCheckpointer` de `src/api/dependencies.py` (esse é um cache de app genérico, não liga com HITL). Junto: um `_THREAD_REGISTRY` com TTL e uma thread de limpeza em background, exatamente como em `query_build/__init__.py:82-123` — evita crescimento indefinido de memória (anti-padrão "memory hoarding"). Consequência aceita: estado de HITL pendente se perde num restart do servidor (mesmo comportamento já existente nos outros 2 agentes com HITL).

## RAG — base de boas práticas Dataform/SQLX

Reaproveita a infraestrutura de embeddings já existente (`src/shared/tools/embeddings.py` — `GoogleGenerativeAIEmbeddings`, `cosine_similarity()`), no mesmo padrão simples do `catalog_index.py` do Finance Auditor (busca por similaridade de cosseno em memória, sem vector DB dedicado — o corpus é pequeno o suficiente).

Corpus: um conjunto curado de snippets sobre convenções Dataform (quando usar `type: "table"` vs `"view"` vs `"incremental"`, uso de `unique_key` em incremental, `assertions` — `uniqueKey`/`nonNull`/`rowConditions`, convenção de nomenclatura de arquivos/tags, uso de `ref()`/`source()`). Precisa ser escrito do zero como parte da implementação — não existe hoje no repositório. Indexado uma vez (script de seed, análogo a `reindex_catalog()`), consultado a cada chamada de `generate_sqlx`.

## Testes

Seguir o harness de eval determinístico já validado nesta sessão (`tests/evals/`, LLM e BigQuery 100% mockados via `ScriptedLLM`/stubs de `dry_run_query`) — criar `tests/evals/cases/case_NN_*.py` cobrindo: conversão simples happy-path, equivalência falhando (deve pausar HITL), score baixo (deve pausar HITL), SQL não-SELECT (deve ser bloqueada em `guardrails_in`). Complementar com testes de API (`tests/api/`) no padrão `TestClient` + `dependency_overrides`/`patch.object` já usado pelos outros agentes.

## Itens em aberto (decidir na implementação, não bloqueiam o design)

- Lista final dos snippets de boas práticas do RAG (rascunho inicial pode ser proposto a partir da documentação oficial do Dataform).
- Valor exato do limite de score de qualidade para acionar o HITL (calibrar como os outros agentes, empiricamente).
- Nome exato do token de cor novo (`indigo` é só uma sugestão) e o ícone SVG de "swap"/transformação.
