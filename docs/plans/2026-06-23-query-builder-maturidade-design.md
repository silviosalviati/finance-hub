# Query Builder — Guardrails, Confiabilidade e Score de Qualidade com HITL

**Data:** 2026-06-23
**Status:** Validado (brainstorm) — pendente de implementação

## Problema

O Query Builder (`src/agents/query_build/`) é um pipeline linear de 5 nós (gera SQL → revisa → valida consistência → dry-run → amostra) sem nenhum dos mecanismos de maturidade que o Finance Voice IA já tem: nenhum bloqueio explícito contra SQL destrutivo, sem RBAC, sem auditoria, sem retry de LLM, sem autocorreção, sem ancoragem de data (o mesmo bug de data que corrigimos no Finance Auditor nesta sessão nunca foi tratado aqui), sem checagem de orçamento antes de rodar, e erros técnicos crus chegam até um público que **não é só técnico** (confirmado com o usuário). Faltava também uma avaliação de qualidade/boas práticas da SQL gerada, com um humano decidindo se aceita ou pede melhoria.

## Solução

Quatro frentes, detalhadas abaixo: (1) extrair guardrails já existentes (Finance Auditor) pra um módulo compartilhado; (2) conectar o Query Builder a eles; (3) melhorar confiabilidade (retry, autocorreção, data, orçamento); (4) erro amigável; (5) nó de score de qualidade com HITL nativo do LangGraph, reaproveitando o padrão já comprovado no `query_analyzer`.

---

## 1. Extração para `src/shared/guardrails/`

Move `rbac.py`, `audit.py`, `pii_guard.py` de `src/agents/finance_auditor/` pra `src/shared/guardrails/`, sem mudar lógica nem nome de tabela SQLite (`finance_user_acl`, `finance_audit_log` continuam iguais — só o código Python muda de pasta, os imports do Finance Auditor são atualizados). Cria mais 2 módulos novos no mesmo lugar, mas extraídos de lógica que já existe:

- **`sql_safety.py`** — extrai o bloqueio de DDL/DML hoje só dentro de `capabilities.py` (`_validate_and_run_sql`) pra uma função pura `assert_select_only(sql) -> str | None`, usada pelos dois agentes.
- **`temporal.py`** — extrai `get_date_block()` (criado nesta sessão pra corrigir a alucinação de data do Composer/Planner) pro shared, já que agora dois agentes precisam dele.

Nenhuma mudança de comportamento nesta etapa — só dar um endereço único e correto a coisas já validadas, antes de conectar o Query Builder.

---

## 2. Query Builder consumindo os guardrails

`QueryBuildState` ganha um campo `user: dict` (hoje não existe nenhum dado de sessão no estado) — sobe até `QueryBuildAgent.analyze()` e a rota da API correspondente.

- **RBAC**: novo nó `check_access`, antes de `generate_sql` — `rbac.check_dataset(user, dataset_hint)`. Roda primeiro de propósito: sem gastar LLM num dataset que o usuário não pode acessar.
- **SQL safety**: entra dentro do `validate_generated_sql_consistency` já existente, usando `sql_safety.assert_select_only()` — mesmo padrão de erro que as checagens de placeholder/coluna que já estão lá.
- **Auditoria**: novo nó `record_audit`, no fim do grafo, sempre roda (sucesso ou erro) — grava pergunta, SQL gerada, custo, usuário e resultado das validações.

---

## 3. Confiabilidade

- **Retry de LLM**: os 2 `llm.invoke()` (gerar e revisar) passam a usar `invoke_with_retry(..., max_attempts=2, label=...)`, já compartilhado — ganho extra: aparecem no log `[llm_timing]` já instrumentado.
- **Autocorreção**: contador `repair_attempts` no estado, limite de 1 retentativa automática — se `validate_sql` ou o dry-run falharem de forma recuperável, volta pro `generate_sql` com o erro específico como contexto (mesmo padrão "TENTATIVA ANTERIOR FALHOU" do `text_to_sql`).
- **Ancoragem de data**: `QUERY_BUILD_SYSTEM_PROMPT` ganha o `temporal.get_date_block()` — data real de hoje + instrução de usar `CURRENT_DATE()`/`DATE_SUB()` pra período relativo, em vez da LLM calcular data sozinha.
- **Orçamento explícito**: `dry_run_generated_sql` compara `bytes_processed` contra um limite configurado **antes** da amostra — se exceder, mensagem clara ("essa consulta processaria X GB, acima do limite — tente um período mais curto") em vez de só reportar custo como número solto.

---

## 4. Erro amigável

Função única `_friendlify_error(erro_bruto, categoria) -> str` em `QueryBuildAgent.analyze()`, antes de devolver a resposta — traduz qualquer erro técnico (exception Python, erro de sintaxe do BigQuery, bloqueio de RBAC) pra linguagem simples, sem jargão. O erro técnico completo **não se perde** — vai inteiro pro `record_audit` (item 2), só não aparece pra quem usa a ferramenta. Determinístico por categoria, sem chamar LLM pra reformular (mais rápido e previsível).

---

## 5. Score de qualidade + HITL (reaproveitando o padrão do `query_analyzer`)

O `query_analyzer` já resolve exatamente este problema noutro contexto: tem um score 0-100 (`_calculate_score`) e um nó de aprovação humana via **`interrupt()` nativo do LangGraph** (não um mecanismo customizado) — o grafo pausa de verdade, processo fica vivo aguardando, e retoma depois via `agent.resume(thread_id=..., human_decision=...)`, exposto na API como `POST /api/agents/query_analyzer/resume`. O Query Builder ganha o mesmo padrão, adaptado:

**Novo nó `score_query`** (depois do dry-run, antes da amostra): avalia a SQL final contra os **5 PILARES OBRIGATÓRIOS já documentados no próprio `QUERY_BUILD_SYSTEM_PROMPT`** (single-scan, sem campo inventado, NULLIF em divisão, CAST explícito, legibilidade) — híbrido regra+LLM, mesmo espírito do `detect_antipatterns` do query_analyzer: checagens determinísticas (regex) pro que é objetivamente verificável (tem `NULLIF`? tem `SELECT *`? `ORDER BY` por posição?), e uma chamada de LLM estruturada pro que exige julgamento (é de fato single-scan? os campos fazem sentido semântico?). Score de 0 a 100, começando em 100 e subtraindo por problema encontrado — mesma lógica do `_calculate_score`, pesos adaptados aos 5 pilares do Query Builder.

**Novo nó `await_quality_approval`**: se `score >= 80`, passa direto (`return {"human_decision": "skip"}`, idêntico ao `await_human_approval` quando não há antipadrão). Se `score < 80`, chama `interrupt({"message": f"A consulta gerada tem nota {score}/100. Deseja seguir assim ou melhorar?", "score": score, "issues": [...]})` — pipeline pausa de verdade ali.

**Resumo via API**: novo endpoint `POST /api/agents/query_build/resume`, espelhando o do `query_analyzer` — `decision: "seguir"` avança pra amostra com o score atual; `decision: "melhorar"` volta pro nó de construção (`generate_sql`, já narrado no prompt como "Engenheiro de Dados Sênior especialista" — a peça "especialista em construção de SQL" que você pediu já é a moldura desse nó) levando o score e os issues específicos como contexto de correção — mesmo padrão de "tentativa anterior falhou" do item 3, agora disparado por qualidade baixa, não só por erro duro.

**Limite de ciclos**: no máximo 2 voltas por "melhorar" — se o score continuar abaixo de 80 depois disso, segue com a melhor versão obtida e avisa isso claramente, em vez de ciclar para sempre.

`QueryBuildAgent` precisa de checkpointer (hoje `build_graph(llm)` não tem; passa a ser `build_graph(llm, checkpointer=...)`, igual ao `query_analyzer`) — é o que permite o `interrupt()` realmente pausar e retomar entre requisições HTTP.

---

## Topologia final do grafo

```
check_access (RBAC)
  → generate_sql (especialista SQL) ──┐
  → review_sql                        │ volta aqui se "melhorar"
  → validate_sql (+ sql_safety)       │ ou erro recuperável
  → dry_run_generated (+ orçamento)   │
  → score_query                       │
  → await_quality_approval ───────────┘ (HITL via interrupt(), só se score < 80)
  → sample_generated
  → record_audit (sempre)
```

## Fora do escopo (YAGNI)

- Persona/narrativa por perfil (Diretor/Gerente/Coordenador) — Query Builder é ferramenta técnica de gerar SQL, não consultor conversacional; não se aplica.
- Reescrever `_calculate_score` do `query_analyzer` para ser 100% genérico entre os dois agentes agora — cada um mede coisas diferentes (otimização de query existente vs. boas práticas de query nova); manter implementações próprias, mas inspiradas no mesmo padrão.
