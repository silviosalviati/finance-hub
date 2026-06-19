const { chromium } = require("playwright");

const SAMPLE_MD = `## Resumo executivo

O faturamento do trimestre fechou em R$ 4,2 milhões, +12,5% em relação ao trimestre anterior, puxado pelo canal Pix.

## Principais achados

- R$ 1,8 milhão vieram de clientes recorrentes — crescimento de 18% no período.
- 3,2% é a nova taxa de inadimplência, queda de 1,1 ponto percentual.
- R$ 245 é o novo ticket médio, alta de 6%.

## Ações recomendadas

- Cobrar os 12 clientes com boleto vencido há mais de 30 dias nas próximas 48h.
- Revisar o limite de crédito dos clientes Pix de alto risco esta semana.
- Acionar o time comercial para renegociação imediata dos 3 maiores contratos em atraso.

## Tabela-resumo

| Canal | Receita | Variação |
| --- | --- | --- |
| Pix | R$ 2,1 milhões | +22% |
| Boleto | R$ 1,3 milhão | -4% |
| Cartão | R$ 0,8 milhão | +9% |

## Próximas perguntas sugeridas

- Quais clientes mais cresceram no período?
- Qual a projeção para o próximo trimestre?
- Onde está concentrado o risco de inadimplência?
`;

function mockData(persona) {
  return {
    status: "ok",
    response_mode: "analysis",
    persona,
    markdown_report: SAMPLE_MD,
    chat_answer: SAMPLE_MD,
    original_query: "como está o faturamento deste trimestre?",
    tool_results: [{ step_index: 0, capability: "text_to_sql", ok: true }],
    artifacts: [],
    token_usage: { total_tokens: 1234 },
  };
}

(async () => {
  const browser = await chromium.launch({ args: ["--no-sandbox"] });
  const page = await browser.newPage({ viewport: { width: 1100, height: 1000 } });
  const errors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") errors.push(msg.text());
  });
  page.on("pageerror", (err) => errors.push(String(err)));

  await page.goto("http://127.0.0.1:8000/", { waitUntil: "networkidle" });

  await page.evaluate(() => {
    showScreen("screen-portal");
    navTo("audit");
    // remove o scroll interno só para a captura — não afeta o app real.
    const area = document.getElementById("fa-messages");
    area.style.overflow = "visible";
    area.style.maxHeight = "none";
  });

  const personas = ["diretor", "gerente", "coordenador", "geral"];
  for (const persona of personas) {
    await page.evaluate(async (data) => {
      document.getElementById("fa-messages").innerHTML = "";
      await appendFABotMessage(data);
    }, mockData(persona));
    await page.waitForTimeout(200);
    await page.screenshot({ path: `.tmp_fa_persona_${persona}.png`, fullPage: true });
  }

  console.log("CONSOLE_ERRORS:", JSON.stringify(errors));
  await browser.close();
})();
