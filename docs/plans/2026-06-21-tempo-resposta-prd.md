# Tempo de Resposta do Finance Voice IA — PRD

**Data:** 2026-06-21
**Status:** Proposto (aguardando priorização)

## Problema

O sistema está funcionalmente maduro, mas a percepção de lentidão na resposta do Finance Voice IA é real, não só percepção: o caminho crítico de uma pergunta encadeia **de 3 a 8 chamadas de LLM sequenciais** (cada uma um round-trip de rede de ~1-3s) mais BigQuery (dry-run + execução = 2 round-trips por query), antes de qualquer texto chegar ao navegador — e aí o frontend ainda soma uma animação de "digitação" artificial de até 6,5s por cima disso.

Hoje não existe instrumentação de tempo por etapa (nenhum `duration_ms`/`perf_counter` em `supervisor.py` ou `audit.py`) — o que significa que qualquer priorização agora é baseada em leitura de código, não em dado de produção. Isso é, em si, o primeiro problema a resolver.

## Objetivo

Reduzir o tempo até o usuário ver conteúdo útil, e reduzir a variância (o "pior caso" hoje é muito pior que o típico). Não é objetivo trocar a arquitetura Plan-and-Execute nem abrir mão das etapas de qualidade (Reflect, dry-run) — é objetivo cortar o que é desperdício puro e medir o resto.

## Linha de base (levantada do código, não medida em produção)

Cadeia sequencial de LLM no caminho típico — **nenhuma dessas etapas roda em paralelo com a anterior, porque cada uma depende da saída da anterior**:

```
Planner (1 LLM, ~1-3s)
  → Router: text_to_sql (1-2 LLM, ~1-3s) → BigQuery dry-run+execute (~1.5-7s)
    → Reflect, SE algo falhou (1 LLM, ~1-2s) → Router de novo
      → Composer (1 LLM, ~1-3s)
        → [resposta completa só agora chega ao navegador]
          → typewriter sintético (900ms-6.500ms) ANTES do usuário ver o texto final
```

| Cenário | Estimado (leitura de código) |
|---|---|
| Típico (sem retry, 1 query) | ~6-8s de backend + até 6,5s de typewriter |
| Pior caso (text_to_sql falha 1x, Reflect dispara replan) | ~17-40s de backend |

Fontes: `supervisor.py:299-306` (Planner), `:744-759` (text_to_sql, structured 2 tentativas + fallback 1 tentativa), `:504-510` (Reflect), `:693-697` (Composer); `capabilities.py:402,414` (dry-run + execute); `scripts.js:4802,4829` (typewriter).

O router **já paralel iza** steps independentes dentro de uma mesma "onda" via `ThreadPoolExecutor` (`supervisor.py:427-434`) — isso já está certo, não é alvo de otimização.

## Causas-raiz, por impacto

1. **Encadeamento sequencial de LLM sem streaming** — o usuário não vê nada até Planner + capabilities + Composer terminarem por completo. Maior fonte de espera percebida.
2. **Typewriter sintético depois que a resposta já chegou completa** — 900ms a 6.500ms de espera **pura e artificial**, somada DEPOIS de toda a espera real. Zero risco para remover/reduzir; é só constante de frontend.
3. **`text_to_sql` pode gastar até 3 tentativas de LLM sequenciais** (structured ×2 + fallback ×1) antes mesmo de rodar a query.
4. **Dry-run + execução = 2 round-trips de BigQuery por query**, sempre, mesmo para SQL repetido/trivial.
5. **Sem cache de schema entre turnos** — `get_table_schema`/`get_dataset_tables_metadata` vão ao BigQuery do zero a cada chamada, mesmo se a mesma tabela já foi descrita há 30 segundos na mesma conversa.
6. **Reindexação do catálogo (embeddings) quando o TTL de 24h expira** — não é o caso comum, mas quando acontece é um pico de latência isolado e visível.

_Não é causa raiz, para não investigar à toa:_ o timer de "pensando" com 5 fases (`scripts.js:5396-5464`) é só UI — ele é cancelado no instante em que a resposta real chega, não adiciona espera nenhuma por conta própria. Se ele "trava" na última fase por 10s, é sintoma da causa #1, não um problema dele mesmo.

## Requisitos propostos, priorizados

### P0 — risco zero, ganho imediato (frontend, sem tocar no backend)

- **Reduzir/remover o atraso artificial do typewriter.** `FA_TYPING_MIN_DURATION_MS` (850ms) e o teto de `targetDurationMs` (6.500ms em `scripts.js:4802`) são constantes — encurtar isso (ou trocar por revelação quase instantânea para respostas longas, mantendo o efeito só em respostas curtas onde ele não atrasa nada) corta até 6,5s percebidos sem nenhum risco técnico.
- **Instrumentar `invoke_with_retry`/`invoke_with_retry_async`** (`llm.py:63-96`) com `time.perf_counter()` antes/depois de cada chamada, e propagar esse tempo para o registro de auditoria (`audit.py`) junto com o nome do nó. É o único ponto por onde toda chamada de LLM já passa — instrumentar ali cobre Planner/Reflect/Composer/text_to_sql de uma vez. **Pré-requisito para priorizar os itens abaixo com dado real em vez de estimativa de leitura de código.**

### P1 — médio esforço, sem mudar a arquitetura

- **Cache de schema de tabela por sessão/TTL curto** (mesmo padrão já usado em `catalog_index.py` com `FINANCE_AUDITOR_CATALOG_TTL_HOURS`), para `get_table_schema`/`get_dataset_tables_metadata` — evita ida ao BigQuery quando a mesma tabela já foi descrita recentemente na mesma conversa.
- **Revisar o orçamento de tentativas do `text_to_sql`**: hoje structured (2) + fallback (1) podem somar 3 chamadas de LLM sequenciais para UM step do plano. Vale medir (com a instrumentação do P0) quantas vezes o fallback de fato dispara — se for raro, não é prioridade; se for frequente, vale entender por quê em vez de só tolerar o custo.
- **Reconsiderar o dry-run obrigatório em toda query.** Ele existe por segurança/estimativa de custo, não é desperdício puro — mas vale avaliar se dá para pular dry-run quando o SQL é estruturalmente idêntico a um já validado nesta sessão (mesmo step do plano, replan), em vez de sempre repetir os dois round-trips.

### P2 — maior esforço, maior ganho percebido

- **Streaming real da resposta do Composer** (Server-Sent Events ou chunked response) em vez de esperar o JSON completo. Isso ataca a causa #1 diretamente: o usuário começaria a ler a resposta enquanto ela ainda está sendo gerada pelo modelo — e tornaria o typewriter sintético do frontend desnecessário (a resposta já "digitaria" no ritmo real do LLM, sem precisar simular nada). É o item de maior impacto na percepção de velocidade, e também o de maior esforço — precisa de mudança no endpoint (`agents.py`) e no client (`scripts.js`, que hoje faz um único `fetch` + `await res.json()`).

## Métricas de sucesso

Não tem como definir meta numérica séria sem a instrumentação do P0 rodando em produção por alguns dias primeiro. Depois de instrumentado, sugestão de acompanhar:
- P50/P90/P95 de tempo total por turno, segmentado por: resposta "chat" (atalho leve) vs "analysis" (pipeline completo), e por número de steps no plano.
- % de turnos em que o Reflect dispara replan (cada disparo = +1 rodada completa de Router e +1 chamada de Reflect).
- % de turnos em que o fallback do `text_to_sql` é acionado.

## Não-objetivos

- Não é objetivo trocar o modelo (`gemini-2.5-flash`) por algo "mais rápido" sem dado — Flash já é o tier rápido do Gemini; trocar de modelo sem medir onde o tempo realmente vai seria chute.
- Não é objetivo remover o Reflect Loop ou o dry-run — são features de qualidade/segurança, não desperdício; a proposta é medir o custo real deles, não eliminá-los de cabeça.
