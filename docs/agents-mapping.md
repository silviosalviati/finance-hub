# Mapeamento de Agentes — Finance Hub IA

Documento de referência para portar/replicar a camada de apresentação (frontend) e a
função de cada agente em outro projeto. Cobre: nome do agente, `agent_id`, view/rota,
função (o que o agente faz), backend (classe/módulo) e os identificadores usados no
frontend (IDs de HTML, classes CSS e funções/variáveis JS).

Fontes no repositório:
- Backend: [src/core/agent_catalog.py](../src/core/agent_catalog.py), [src/agents/](../src/agents/), [src/api/dependencies.py](../src/api/dependencies.py)
- Frontend: [static/index.html](../static/index.html), [static/css/style.css](../static/css/style.css), [static/js/scripts.js](../static/js/scripts.js)

---

## Visão geral (catálogo)

| `agent_id` | `display_name` | `view` | `icon_token` | `color_token` | Classe Python |
|---|---|---|---|---|---|
| `query_analyzer` | SQL Review | `qa` | `search` | `porto` | `QueryAnalyzerAgent` |
| `document_build` | Document Builder | `db` | `file` | `teal` | `DocumentBuildAgent` |
| `query_build` | Query Builder | `qb` | `branch` | `violet` | `QueryBuildAgent` |
| `schema_graph` | Schema Explorer | `er` | `diagram` | `emerald` | `SchemaGraphAgent` |
| `finance_auditor` | Finance Voice IA | `audit` | `shield` | `amber` | `FinanceAuditorAgent` |
| `query_transformer` | Query Transformer | `qt` | `swap` | `violet` | `QueryTransformerAgent` |

Todos os agentes implementam a interface comum `BaseAgent` ([src/core/base_agent.py](../src/core/base_agent.py))
com as propriedades `agent_id` e `display_name`, e são registrados em runtime pelo
`AgentRegistry` ([src/core/registry.py](../src/core/registry.py)) via `get_registry()`
em [src/api/dependencies.py](../src/api/dependencies.py).

Metadados de UI (nome exibido, descrição, ícone, cor, tags cosméticas) ficam
centralizados em `AGENT_CATALOG` ([src/core/agent_catalog.py](../src/core/agent_catalog.py)),
separados da lógica de execução — o card do agente no frontend é montado 100% a
partir desse dicionário + do payload retornado por `GET /api/agents`.

---

## 1. SQL Review (`query_analyzer`)

**Função:** revisa uma query SQL do BigQuery já escrita pelo usuário, avaliando custo,
performance, antipadrões e sugerindo otimizações (fluxo de "SQL Review" com HITL —
Human-in-the-loop — para aprovar/pular a otimização sugerida).

- Backend: `QueryAnalyzerAgent` — [src/agents/query_analyzer/\_\_init\_\_.py](../src/agents/query_analyzer/__init__.py)
- Grafo: [src/agents/query_analyzer/graph.py](../src/agents/query_analyzer/graph.py)
- View/rota (`navTo`): `qa` → `#view-qa`

### HTML (IDs)
| ID | Elemento |
|---|---|
| `view-qa` | `<section class="view">` raiz da tela |
| `qa-query` | textarea de entrada da SQL |
| `qa-btn` / `qa-btn-text` / `qa-spinner` | botão "Analisar com SQL Review" |
| `qa-error` | caixa de erro |
| `qa-progress` / `qa-progress-step` / `qa-progress-fill` | barra de progresso |
| `qa-ctx-indicator` / `qa-ctx-title` / `qa-ctx-message` | indicador de validação de contexto (dataset/tabelas) |
| `qa-ctx-icon-checking` / `qa-ctx-icon-ok` / `qa-ctx-icon-error` | ícones de estado do indicador |
| `qa-last-run` / `qa-last-time` / `qa-last-score` | resumo da última execução |
| `qa-hitl-panel` / `qa-hitl-cost` / `qa-hitl-antipatterns` | painel de aprovação humana (HITL) |
| `qa-hitl-approve` / `qa-hitl-skip` | botões de decisão HITL |
| `qa-hitl-processing` / `qa-hitl-proc-title` / `qa-hitl-proc-desc` | estado "processando" pós-decisão |
| `qa-empty` | estado vazio |
| `qa-tabs-area` | área de resultado com abas |
| `qa-copy-sql-btn` | botão de copiar SQL |
| `nav-qa` | item de menu lateral (ativado via JS, não presente estaticamente no HTML) |

### CSS (classes)
`qa-ctx-box`, `qa-ctx-box-title`, `qa-ctx-box-msg`, `btn-analyze`, `spinner`,
`qa-error`, `qa-progress`, `qa-progress-step`, `qa-progress-fill`,
`qa-progress-indeterminate`, `qa-last-run`, `hitl-panel`, `hitl-cost-badge`,
`ap-list`, `hitl-action-btn`, `hitl-action-primary`, `hitl-action-secondary`,
`hitl-processing`, `hitl-proc-title`, `hitl-proc-desc`, `qa-empty`,
`qa-tab-panel`, `copy-btn`, `copied`.

### JS (funções/variáveis relevantes)
`runAnalyze()`, `resumeQA(decision)`, `copySQL(event)`, `setQAProgress(text, pct)`,
`startQAIndeterminateFallback()`, `clearQAIndeterminateFallback()`, `hideQAProgress()`,
vars: `qaIsLoading`, `qaAnalyzeInFlight`, `_qaHitlThreadId`, `_qaLastResult`,
`qaDatasetValidationState`, `qaDatasetValidationTimer`.

---

## 2. Query Builder (`query_build`)

**Função:** gera SQL do BigQuery a partir de linguagem natural (NL2SQL), com seleção
de gerência/dataset, dry-run automático, nota de qualidade, estimativa de custo e
fluxo HITL para melhorar ou aceitar a query gerada.

- Backend: `QueryBuildAgent` — [src/agents/query_build/\_\_init\_\_.py](../src/agents/query_build/__init__.py)
- Grafo: [src/agents/query_build/graph.py](../src/agents/query_build/graph.py)
- View/rota: `qb` → `#view-qb`

### HTML (IDs)
| ID | Elemento |
|---|---|
| `view-qb` | raiz da tela |
| `qb-gerencia-picker` / `qb-gerencia-picker-head` / `qb-gerencia-picker-title` / `qb-gerencia-picker-hint` / `qb-gerencia-picker-list` | seletor de gerência/área de negócio |
| `qb-area-pill` / `qb-area-pill-icon` / `qb-area-pill-label` / `qb-area-pill-change` | pill com a área selecionada |
| `qb-config-label`, `qb-project-field`, `qb-project`, `qb-dataset-field`, `qb-dataset` | configuração de projeto/dataset |
| `qb-dataset-indicator`, `qb-dataset-status`, `qb-dataset-status-icon`, `qb-dataset-status-title`, `qb-dataset-status-text`, `qb-dataset-status-meta` | validação do dataset |
| `qb-generating` | painel central de geração (loading) |
| `qb-gen-phase`, `qb-gen-sub`, `qb-gen-timer` | textos do painel de geração |
| `qb-hitl-panel`, `qb-hitl-subtitle`, `qb-hitl-issues` | painel HITL de qualidade |
| `qb-hitl-improve` / `qb-hitl-accept` | botões de decisão HITL |
| `qb-hitl-processing`, `qb-hitl-proc-title`, `qb-hitl-proc-desc` | estado pós-decisão |
| `qb-empty` | estado vazio |
| `qb-gerencia-learning` | dica/aprendizado contextual |
| `qb-tabs-area`, `qb-tab-score`, `qb-tab-premises`, `qb-tab-optimized`, `qb-tab-recs` | abas de resultado |
| `qb-panel-score`, `qb-grade-block`, `qb-grade-ltr`, `qb-score-big`, `qb-score-fill`, `qb-summary` | aba de nota/score |
| `qb-tiles`, `qb-cost-est`, `qb-bytes-proc`, `qb-bytes-proc-sub`, `qb-cost-tier-badge`, `qb-cost-tier-sub` | tiles de custo/bytes |
| `qb-sav-sec`, `qb-sav-big`, `qb-sav-fill` | economia estimada |
| `qb-panel-premises`, `qb-built-sql` | SQL construída |
| `qb-panel-optimized`, `qb-opt-empty`, `qb-opt-sec`, `qb-sample-note`, `qb-sample-wrap`, `qb-sample-head`, `qb-sample-body` | SQL otimizada + amostra |
| `qb-panel-recs`, `qb-rec-sec`, `qb-recs-list`, `qb-tips-sec`, `qb-dryrun` | recomendações e dry-run |
| `qb-suggestions-block`, `qb-suggestions-extra`, `qb-suggestions-toggle` | sugestões de perguntas |
| `qb-error` | erro |
| `qb-request-field`, `qb-request`, `qb-btn` | input de solicitação e botão gerar |
| `nav-qb` | item de menu (ativado via JS) |

### CSS (classes)
`qb-area-pill`, `qb-area-pill-icon`, `qb-area-pill-label`, `qlabel`, `qfield`,
`grade-block`, `grade-ltr`, `score-big`, `score-txt`, `tiles`, `tile-val`, `tile-sub`,
`sav-pct`, `sav-fill`, `qa-opt-empty`, `qb-sample-wrap`, `rec-list`,
`fa-sidebar-head`, `fa-sidebar-title`, `fa-sidebar-hint`, `fa-suggestions`,
`fa-suggestions-extra`, `fa-suggestions-toggle`, `fa-suggestions-icon`,
`fa-thinking-dots`, `hitl-panel`, `hitl-action-btn`, `qa-tab-panel`,
`qb-input-row`.

### JS (funções/variáveis relevantes)
`runQueryBuild()`, `resumeQB(decision)`, funções de geração de painel central
(fases `_QB_GEN_PHASES`: `validating`, `generating`, `dryrun`, `reviewing`),
vars: `qbIsLoading`, `_qbHitlThreadId`, `qbDatasetValidationState`,
`qbDatasetValidationTimer`, `_qbGenTimer`, `_qbGenSeconds`.

---

## 3. Document Builder (`document_build`)

**Função:** gera documentação técnica/funcional de uma tabela do BigQuery (schema +
regras de negócio) em três formatos: Markdown, HTML e Confluence, com checklist de
qualidade e nota geral.

- Backend: `DocumentBuildAgent` — [src/agents/document_build/\_\_init\_\_.py](../src/agents/document_build/__init__.py)
- Grafo: [src/agents/document_build/graph.py](../src/agents/document_build/graph.py)
- View/rota: `db` → `#view-db`

### HTML (IDs)
| ID | Elemento |
|---|---|
| `view-db` | raiz da tela |
| `db-request` | input de solicitação (tabela/contexto) |
| `db-btn` / `db-btn-text` / `db-spinner` | botão "Gerar com Document Builder" |
| `db-error` | erro |
| `db-progress`, `db-progress-step`, `db-progress-fill` | barra de progresso |
| `db-empty` | estado vazio |
| `db-tabs-area`, `db-tab-score`, `db-tab-structure`, `db-tab-document`, `db-tab-html`, `db-tab-confluence`, `db-tab-checklist` | abas de resultado |
| `db-panel-score`, `db-grade-block`, `db-grade-ltr`, `db-score-big`, `db-score-fill`, `db-summary` | aba de nota |
| `db-tiles`, `db-doc-type`, `db-sections-count`, `db-checklist-count` | tiles resumo |
| `db-panel-structure`, `db-structure-list` | estrutura sugerida |
| `db-panel-document`, `db-markdown`, `db-copy-btn` | documento em Markdown |
| `db-panel-html`, `db-html-preview`, `db-html-source`, `db-copy-html-btn` | saída HTML |
| `db-panel-confluence`, `db-confluence-source`, `db-copy-confluence-btn` | saída para Confluence |
| `db-panel-checklist`, `db-checklist-list`, `db-next-steps-sec`, `db-next-steps-list` | checklist e próximos passos |
| `nav-db` | item de menu (ativado via JS) |

### CSS (classes)
`qa-progress`, `qa-empty`, `qa-tab-panel`, `db-html-panel`, `grade-block`,
`grade-ltr`, `score-big`, `tiles`, `tile-val`, `rec-list`, `copy-btn`.

### JS (funções relevantes)
`runDocumentBuild()`.

---

## 4. Schema Explorer (`schema_graph`)

**Função:** introspecta o BigQuery e monta um grafo entidade-relacionamento (ER)
interativo (D3 + Dagre) com os relacionamentos inferidos entre tabelas de um ou mais
datasets.

- Backend: `SchemaGraphAgent` — [src/agents/schema_graph/\_\_init\_\_.py](../src/agents/schema_graph/__init__.py)
- Grafo: [src/agents/schema_graph/graph.py](../src/agents/schema_graph/graph.py)
- View/rota: `er` → `#view-er`
- Item de menu lateral fixo (não é card do grid): `nav-er` (ícone de "diagram" inline no HTML)
- Dependências JS externas: `static/js/d3.v7.min.js`, `dagre.min.js` (CDN)

### HTML (IDs)
- `view-er` (raiz — estrutura interna não detalhada neste levantamento; usa
  `d3.v7.min.js` para renderizar o SVG do grafo).
- `nav-er` — item do menu lateral.

### JS
Usa D3.js (`static/js/d3.v7.min.js`) e Dagre (CDN) para layout/renderização do grafo;
lógica de app específica dentro de `scripts.js` (funções de fetch/parse do grafo de
schema, prefixo característico não padronizado como os demais agentes).

---

## 5. Finance Voice IA (`finance_auditor`)

**Função:** assistente conversacional (Supervisor + Specialists) sobre os dados
financeiros — permite perguntas livres, encadeando dinamicamente capabilities
(consulta BigQuery, texto-para-SQL, estatísticas, gráficos, métricas, forecast,
memória organizacional, análise de anexos, podcast/áudio) sem domínio fixo.

- Backend: `FinanceAuditorAgent` — [src/agents/finance_auditor/\_\_init\_\_.py](../src/agents/finance_auditor/__init__.py)
- Supervisor/grafo: [src/agents/finance_auditor/supervisor.py](../src/agents/finance_auditor/supervisor.py)
- Schemas do planner: [src/agents/finance_auditor/supervisor_schemas.py](../src/agents/finance_auditor/supervisor_schemas.py)
- Capabilities: `bq_list_datasets`, `bq_list_tables`, `bq_get_schema`, `bq_query`,
  `text_to_sql`, `stats_describe`, `viz_spec`, `metric_lookup`, `metric_execute`,
  `org_fact_save`, `org_fact_recall`, `forecast_simple`, `attachment_analyze`,
  `chat_answer`, `catalog_search`
- View/rota: `audit` → `#view-fa`

### HTML (IDs)
| ID | Elemento |
|---|---|
| `view-fa` | raiz da tela (chat) |
| `nav-audit` | item de menu (ativado via JS) |

> Observação: o levantamento automático não expandiu todos os IDs internos do chat
> (mensagens, anexos, opções de voz/tom, gráficos) por serem gerados dinamicamente
> via JS a partir de templates de string. Prefixo predominante no JS: `fa-`.

### CSS (classes, prefixo `fa-`)
`fa-thinking-dots`, `fa-chart-error-icon`, `fa-delta`, `fa-urgency-chip`,
`fa-action-check`, `fa-voice-icon-glyph`, `fa-voice-icon-play`, `fa-tone-icon-glyph`,
`fa-sidebar-head`, `fa-sidebar-title`, `fa-sidebar-hint`, `fa-suggestions*`.

### JS (funções relevantes)
`_faIcon(name, size)`, `_faIconizeNextStep(report)`, funções de renderização de
cartões de métrica, gráfico, voz e tom (chips de capability: `grid`, `code`,
`database`, `bar-chart`, `volume-2`), vars: `auditIsLoading`, `auditMarkdownCache`.

---

## 6. Query Transformer (`query_transformer`)

**Função:** converte SQL do BigQuery em modelos `.sqlx` do Dataform, seguindo boas
práticas de mercado, com validação de sintaxe, dry-run do modelo gerado, comparação
de equivalência (schema/resultado) e fluxo de perguntas de requisitos (materialização)
antes da conversão.

- Backend: `QueryTransformerAgent` — [src/agents/query_transformer/\_\_init\_\_.py](../src/agents/query_transformer/__init__.py)
- Grafo: [src/agents/query_transformer/graph.py](../src/agents/query_transformer/graph.py)
- View/rota: `qt` → `#view-qt`

### HTML (IDs)
| ID | Elemento |
|---|---|
| `view-qt` | raiz da tela |
| `qt-sql` | textarea de SQL de entrada |
| `qt-line-count` | contador de linhas |
| `qt-btn` / `qt-btn-text` / `qt-spinner` | botão "Converter para SQLX" |
| `qt-error` | erro |
| `qt-progress-state`, `qt-progress-title`, `qt-progress-subtitle`, `qt-progress-steps` | progresso do pipeline |
| `qt-requirements-state`, `qt-requirements-message`, `qt-recommendation`, `qt-questions`, `qt-requirements-submit` | formulário de requisitos (materialização) |
| `qt-question-{id}` (dinâmico) | campos de pergunta gerados por `data-qt-question` |
| `qt-empty` | estado vazio |
| `qt-hitl-panel`, `qt-hitl-subtitle`, `qt-hitl-issues`, `qt-hitl-sqlx` | painel HITL de qualidade |
| `qt-hitl-improve` / `qt-hitl-accept` | botões de decisão HITL |
| `qt-result` | contêiner de resultado (abas) |
| `qt-overview-status`, `qt-overview-materialization`, `qt-overview-quality`, `qt-overview-equivalence` | cartões de visão geral |
| `qt-report` | relatório textual |
| `qt-copy-btn`, `qt-sqlx-output` | SQLX gerado + cópia |
| `qt-original-project`, `qt-original-sql` | SQL original + metadado de projeto |
| `qt-validation-business-title`, `qt-validation-business-text`, `qt-validation-bytes`, `qt-validation-cost`, `qt-validation-equivalence`, `qt-validation-detail` | validação/negócio |
| `nav-qt` | item de menu (ativado via JS) |

### CSS (classes)
`qt-progress-state`, `qt-progress-steps`, `qt-progress-step` (+ modificadores
`is-active`, `is-done`), `qt-requirements-state`, `qt-requirements-submit`,
`qt-question`, `qt-tabs-area`, `qt-metric-card`, `qt-metric-icon` (+ `is-green`,
`is-blue`, `is-amber`, `is-violet`), `qt-code-actions`, `qt-copy-btn`,
`qt-results-eyebrow`, `qt-validation-card`, `qt-validation-card-wide`, `hitl-panel`.

### JS (funções/variáveis relevantes)
`runQueryTransformer()`, `qtSubmitRequirements()`, `qtResumeQuality(decision)`,
`copyQTSQLX()`, `copyQTOriginalSQL()`, `_qtSetProgressStep(step)`,
`_qtUpdateInputMeta()`, `_QT_PROGRESS_PHASES` (array de fases),
vars: `_qtHitlThreadId`, `_qtProgressTimer`, `_qtProgressStep`.

---

## Estrutura comum de navegação (home grid)

Todos os agentes (exceto o Schema Explorer, que tem item fixo no menu) aparecem como
cartões no grid da home, renderizados por `_renderBotCard(a)` em
[static/js/scripts.js](../static/js/scripts.js):

```html
<article class="bot-card" data-color="{color_token}" onclick="navTo('{view}')">
  <div class="bhead">
    <div class="biw bi-{color_token}">{svg do icon_token}</div>
    <span class="bstatus st-live">DISPONÍVEL</span>
  </div>
  <div>
    <div class="bname">{display_name}</div>
    <div class="bdesc">{description}</div>
  </div>
  <div class="bfoot">
    <span class="tag-badge"><span class="tag-badge-label">{tag}</span></span>
    <button class="btn-open">Acessar {svg seta}</button>
  </div>
</article>
```

- Container: `#bot-grid` → `.bot-grid` (estado de carregamento: `.bot-card.skeleton`)
- Cores por `data-color`: `porto`, `teal`, `violet`, `emerald`, `amber`
  (classes CSS: `.bot-card[data-color="..."]`, `.biw.bi-{cor}`)
- Ícones (`AGENT_ICONS`, mapa `icon_token → SVG` inline): `search`, `file`, `branch`,
  `diagram`, `shield`, `swap`
- Função de carregamento: `loadAccessibleAgents()` (chama `GET /api/agents`),
  `renderBotGrid()`, `_agentPrimaryTag(a)`
- Roteamento central: `navTo(view)` — mapeia `view` → `id` da seção (`.view.active`)
  e ativa o item correspondente do menu (`.nitem.active`):

```js
const mapping = {
  home: "view-home",
  qa: "view-qa",
  db: "view-db",
  qb: "view-qb",
  audit: "view-fa",
  qt: "view-qt",
  er: "view-er",
  dev: "view-dev",
  hist: "view-hist",
  "admin-users": "view-admin-users",
  "admin-config": "view-admin-config",
  "admin-agents": "view-admin-agents",
};
```

### Convenções de nomenclatura observadas
- Cada agente usa um **prefixo curto** consistente em IDs/JS: `qa-` (SQL Review),
  `qb-` (Query Builder), `db-` (Document Builder), `qt-` (Query Transformer),
  `fa-` (Finance Auditor). Schema Explorer (`er`) não segue esse padrão de prefixo.
- Padrão de tela: `<section class="view" id="view-{view}">`.
- Padrão de item de menu: `<div class="nitem" id="nav-{view}">`.
- Painéis HITL (Human-in-the-loop) reaproveitam a classe genérica `.hitl-panel`,
  `.hitl-action-btn` (`.hitl-action-primary` / `.hitl-action-secondary`),
  `.hitl-processing` em praticamente todos os agentes que pausam para aprovação
  humana (`query_analyzer`, `query_build`, `query_transformer`, `finance_auditor`).
- Abas de resultado reaproveitam `.qa-tab-panel` como classe genérica em vários
  agentes (não só no SQL Review).
