# Query Builder — Gerência no Cadastro, Layout de Custo e Novos Pilares de Performance

**Data:** 2026-06-24
**Status:** Validado (brainstorm) — pendente de implementação

## Problema

O Query Builder hoje só é alcançável navegando pelo Schema Explorer (clicar numa tabela → "Gerar insights analíticos") — não tem entrada direta, o que é fricção pra quem só quer gerar uma query pra área em que trabalha. Não existe nenhum vínculo entre o cadastro do usuário e uma gerência/área de negócio, então o sistema não sabe "de qual área é esse usuário" pra agilizar esse fluxo. Além disso, a tela de resultado trata custo/performance como informação secundária (dois blocos hoje mostram o mesmo número de bytes duplicado, herdados de um layout pensado pro Query Analyzer), e o conjunto de boas práticas verificadas (5 pilares + score) ainda não cobre particionamento/clustering — exatamente o tipo de regra que mais impacta custo no BigQuery.

## Solução

Quatro frentes: (1) campo de gerência no cadastro do usuário; (2) Query Builder com entrada direta e fixa por gerência, sem passar pelo Schema Explorer; (3) layout do resultado com custo/performance em destaque; (4) duas novas regras de pontuação (particionamento, clustering) mais um sinal visual de faixa de custo.

---

## 1. Cadastro: campo "gerência" no usuário

Nova coluna `gerencia TEXT` na tabela `users` já existente (`src/core/database.py`) — não é uma tabela nova, é mais um atributo do cadastro, igual `name`/`is_admin`.

No modal de admin (`adminOpenUserModal`/`adminSaveUser`, hoje só com username/nome/senha/checkbox admin), entra um **dropdown** de gerência — não texto livre — populado com os valores reais de gerência que já existem como label de dataset no BigQuery, reaproveitando o mecanismo que hoje alimenta a sugestão de gerências do Finance Auditor (`_gerencia_catalog_cache`/`resolve_dataset_by_gerencia` em `src/agents/finance_auditor/capabilities.py`). O admin escolhe de uma lista validada; nunca digita algo que não bate com nenhum dataset real.

A sessão do usuário (`session`/`currentUser`, já carregada no login) passa a incluir `gerencia`, do mesmo jeito que já inclui `username`/`is_admin`.

**Projeto GCP**: como o novo fluxo pula o Schema Explorer (hoje a única fonte do `project_id`), uso `FINANCE_AUDITOR_DEFAULT_PROJECT` (config já existente, default `"silviosalviati"`) como projeto padrão pra resolver o dataset da gerência — sem precisar de um campo de projeto por usuário.

---

## 2. Fluxo de entrada do Query Builder

Hoje `runQueryBuild()` exige `qbDatasetValidationState.status === "valid"`, que só fica `valid` depois de passar pelo Schema Explorer. Ao abrir a aba Query Builder (`navTo('qb')`), passa a verificar `currentUser.gerencia`:

- **Usuário comum com gerência cadastrada**: chama `resolve_dataset_by_gerencia(default_project, gerencia)` automaticamente e popula `qbDatasetValidationState` direto como `valid`, sem Schema Explorer. Os campos de Project ID/Dataset saem do formulário; em vez disso, um badge fixo somente leitura "Gerência: Cobrança" no topo, deixando claro o contexto sem parecer editável.
- **Admin (`is_admin=true`)**: comportamento atual inalterado — campos de projeto/dataset visíveis e editáveis, preenchidos pelo Schema Explorer como hoje.
- **Usuário comum sem gerência cadastrada**: cai no fluxo atual (Schema Explorer obrigatório) — zero quebra durante a migração gradual dos cadastros.

Importante: isso é **só conveniência de UX**, não toca em permissão. O RBAC (`check_access`/`finance_user_acl`, já implementado nas Etapas 1-3 da maturidade do Query Builder) continua sendo a autoridade real — a gerência só pré-popula o campo. Se o RBAC negar o dataset da própria gerência do usuário, `check_access` bloqueia normalmente e o erro amigável já existente aparece.

---

## 3. Layout: performance/custo em destaque

A aba "Score" já mostra grade/nota num box colorido (mantém como está). Embaixo, os 3 blocos atuais — "Bytes estimados" e "Bytes validados" (mesmo número duplicado, herdado do layout do Query Analyzer que compara original vs. otimizado — não se aplica ao Query Builder) e "Nível de qualidade" espremido como texto — são substituídos por três sinais distintos:

1. **Custo estimado** (R$, fonte grande, primeiro a saltar aos olhos)
2. **Bytes processados** (GB/MB, contextualizado)
3. **Nível de custo** — badge próprio (🟢 Baixo / 🟡 Moderado / 🔴 Alto), **separado** da nota de qualidade A-F (ver Seção 4)

Logo abaixo do box de grade, uma frase única em linguagem simples (ex.: *"Esta consulta deve processar 780 MB (≈ R$ 0,003) — dentro do esperado."*), substituindo o uso do texto cru de `data.explanation` como única informação dessa área.

O resto do layout (abas Query construída / Amostra de dados / Recomendações) fica inalterado.

---

## 4. Novos pilares de boas práticas

Duas regras novas no mesmo mecanismo de score já existente (`_calculate_quality_score`/`score_query`, regras determinísticas, sem custo extra de LLM):

- **Particionamento obrigatório** (penalidade forte, ex. -20): se a tabela referenciada tem `partition_field` conhecido (já coletado hoje no schema/contexto passado à LLM) e a SQL gerada não filtra essa coluna no WHERE, é o cenário mais caro possível no BigQuery (varredura completa numa tabela pensada pra ser filtrada).
- **Uso de clustering** (penalidade leve, ex. -5): se a tabela tem `clustering_fields` e nenhum aparece no WHERE/ORDER BY, é uma oportunidade de eficiência perdida — mais sugestão que erro.

**Faixa de custo** (liga com a Seção 3, **não** afeta a nota 0-100): abaixo de 20% do orçamento configurado (`QUERY_BUILD_BUDGET_BYTES`) = 🟢 Baixo; 20-70% = 🟡 Moderado; acima de 70% (mas ainda dentro do limite) = 🔴 Alto. Não é "má prática" — o usuário pode legitimamente precisar processar muito dado — então é só informativo, alimentando o badge da Seção 3, não a pontuação.

---

## Fora do escopo (YAGNI)

- Gerência como fonte de verdade do RBAC (substituir/alimentar `finance_user_acl` automaticamente) — descartado: o usuário confirmou que o objetivo é só conveniência de UX, RBAC continua 100% manual via ACL como hoje.
- Seletor de troca de gerência dentro do Query Builder — descartado: gerência do cadastro é fixa; quem precisar de outra área pede ao admin pra mudar o cadastro.
- Aplicar essa mesma pré-seleção por gerência no fluxo do Finance Auditor — descartado: escopo fica restrito ao Query Builder.
- Campo de projeto GCP por usuário — descartado: todo mundo usa o mesmo projeto (`FINANCE_AUDITOR_DEFAULT_PROJECT`), não há necessidade real hoje.
- Faixa de custo afetando a nota de qualidade — descartado: custo alto não é necessariamente má prática, é só informativo.
