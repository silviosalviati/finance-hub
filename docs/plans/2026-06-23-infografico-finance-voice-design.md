# Infográfico exportável a partir de uma análise — Design

**Data:** 2026-06-23
**Status:** Validado (brainstorm) — pendente de implementação

## Problema

Hoje, depois de uma análise do Finance Voice IA, a única forma de levar aquele resultado pra fora do chat é copiar texto ou um gráfico Vega-Lite isolado. Não existe uma saída visual única, resumida e exportável (imagem) que alguém possa baixar e compartilhar/anexar (e-mail, apresentação pro board) sem reescrever nada manualmente.

## Solução

Uma nova capability, `generate_infographic`, dentro do Finance Voice IA já existente — não um agente separado. Reaproveita a análise já entregue na resposta anterior (sem rodar BigQuery de novo), extrai os destaques certos e gera uma imagem PNG com layout de dashboard corporativo, usando a paleta/tipografia Porto Seguro.

---

## Arquitetura

```
Usuário (chat): "gera um infográfico dessa análise"
  → guardrails_in → persona_resolver → response_mode_resolver
  → planner (já documentado com a nova capability, escolhe sozinho — igual escolhe viz_spec hoje)
  → router → cap_generate_infographic(args, context)
       context["previous_analysis"] = última resposta completa da sessão
       (markdown_report + tool_results + artifacts)
  → composer (narra a entrega) → guardrails_out
  → artifact {"type": "infographic", "image_url": "..."}
```

**Por que capability, não agente novo**: a saída não precisa de Planner/Router próprio (não busca dado novo) — só processa o que já existe. Vive no mesmo `CAPABILITY_REGISTRY` de `capabilities.py`, ao lado de `viz_spec`/`text_to_sql`.

**Correção necessária no roteamento**: existe um filtro antes do Planner (`_is_analytics_query` em `agents.py`) que decide entre o pipeline completo e um atalho de chat leve sem acesso a capabilities — o mesmo mecanismo que causou o bug "viz_spec não existe" investigado nesta sessão. Adicionar "infográfico"/"infografico" à lista de palavras-chave desse filtro é obrigatório; sem isso, o pedido nunca chega ao Planner.

**Fiação de dados nova**: `chat_session["turns"][-1]["response"]` (já persistido no checkpointer) precisa ser passado como `previous_analysis` no `initial_state` que `agents.py` monta antes de chamar `agent.analyze(...)`, e incluído em `SupervisorState` pra chegar no `context` da capability.

---

## Extração de conteúdo

Uma chamada de LLM leve (structured output, mesmo padrão do Planner/Reflect) lê `previous_analysis.markdown_report` + `.tool_results` e devolve um JSON com:
- Título + subtítulo (período/contexto).
- 3-4 KPIs (número grande + legenda + variação + cor semântica: emerald/rose/amber).
- Mini-gráfico: reaproveita o artifact `vega_lite` já existente na análise original, se houver (sem reprocessar dado).
- Lista de achados com badge de status (ex. "BAIXO RISCO").
- Conclusão de 1 linha.

Não inventa número novo — só seleciona/organiza o que o Composer já calculou e narrou.

---

## Renderização visual

Template HTML novo e dedicado, usando os tokens de design já existentes (cores Porto Seguro, fontes Sora/DM Sans, sombras, raio de borda) — não o `index.html` inteiro. Layout segue o briefing "Estilo Dashboard": cards modulares arredondados com sombra sutil, hierarquia número-grande/legenda-pequena, sparklines/mini-gráficos, ícones monocromáticos, grid com espaçamento generoso, divisórias sutis.

**Geração via HTML/CSS real + screenshot (Playwright), não geração de imagem por IA** — decisão deliberada: modelos generativos de imagem alucinam texto/números, e um valor financeiro errado numa imagem é um risco real de credibilidade. HTML real garante que os números são texto exato, não pixels adivinhados.

Pipeline: monta HTML (valores escapados) → Playwright (API síncrona) abre num Chromium headless → espera o Vega-Lite (se houver) terminar de renderizar → screenshot → PNG.

**Dependência nova de produção**: Chromium via Playwright precisa ser instalado no ambiente de deploy. Considerar manter uma instância de browser viva entre chamadas (evita ~300-500ms de startup por requisição).

---

## Saída e integração no frontend

PNG salvo em `.sixth/infographics/<uuid>.png`, servido por uma rota nova (`GET /api/agents/finance_auditor/infographic/{id}`) — artifact carrega só a URL, não base64 (mais leve, link estável pra compartilhar/baixar).

No frontend, um novo `case "infographic":` em `_faRenderArtifact` (`scripts.js`) renderiza `<img>` + botão "Baixar", reaproveitando 100% a casca de card de artifact que já existe (animação, allowlist `_FA_ANSWER_CAPS` — só precisa incluir `generate_infographic` nele). Nenhuma tela ou fluxo de UI novo.

---

## Tratamento de erro

- **Sem análise anterior na sessão** (primeira mensagem, ou turno anterior foi o atalho de chat leve sem dado): capability devolve erro claro; Composer explica em linguagem de negócio ("ainda não tenho uma análise pra transformar em infográfico — peça uma análise primeiro") em vez de travar.
- **Extração ou renderização falha** (Chromium não inicia, timeout, LLM de extração falha): erro tratado, Composer descreve o que já se sabe e oferece tentar de novo — mesmo padrão das REGRAS ANTI-META-RESPOSTA já usadas em todo o resto do Finance Voice IA (nunca expõe jargão técnico).

## Fora do escopo (YAGNI — registrado, não construído agora)

- **Botão de atalho por mensagem**: descartado a favor de gatilho só por texto. Só voltaria a fazer sentido se usuários pedirem para targetar uma análise específica de várias mensagens atrás (hoje sempre usa "a última").
- **Limpeza automática de PNGs gerados** em `.sixth/infographics/`: sem TTL/rotina de exclusão por ora; resolver se o volume em disco crescer.
- **Geração por IA de imagem** como alternativa/fallback: descartada pelo risco de números incorretos.

---

## Pronto pra implementação?

Esse design cobre arquitetura, extração de conteúdo, renderização, integração e erro — fica como próximo passo formal um plano de implementação detalhado (arquivos exatos a tocar, ordem dos passos, testes).
