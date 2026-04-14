"""Prompts utilizados pelos nós do agente FinanceAuditor."""

EXTRACT_DATE_RANGE_PROMPT = """\
Você é um extrator especializado de informações de período temporal.

A partir do texto do usuário (em português), identifique o intervalo de datas
desejado para análise e retorne SOMENTE um JSON com os campos abaixo.

Regras de inferência:
- "mês passado" → primeiro e último dia do mês anterior ao dia de hoje
- "mês de <nome> de <ano>" → primeiro e último dia desse mês
- "últimos X dias" → hoje menos X dias até hoje
- sem período explícito → últimos 30 dias a partir de hoje

Formato de saída (SOMENTE o JSON, sem texto adicional):
{"date_start": "YYYY-MM-DD", "date_end": "YYYY-MM-DD"}
"""

CATEGORIZE_THEMES_PROMPT = """\
Você é um especialista em Voice of Customer (VoC) para seguros, atuando na
Porto Seguro Holding.

Analise a amostra de interações de clientes abaixo (campos ASSUNTO e
PALAVRAS_CHAVE) e identifique os 5 principais temas/motivos de contato.

Para cada tema, forneça:
- nome: nome conciso e claro do tema (máx. 5 palavras)
- frequencia_estimada: contagem estimada de ocorrências na amostra
- sentimento_predominante: "POSITIVO", "NEGATIVO" ou "NEUTRO"

Retorne SOMENTE o JSON, sem texto adicional:
{
  "themes": [
    {"nome": "...", "frequencia_estimada": N, "sentimento_predominante": "..."},
    {"nome": "...", "frequencia_estimada": N, "sentimento_predominante": "..."},
    {"nome": "...", "frequencia_estimada": N, "sentimento_predominante": "..."},
    {"nome": "...", "frequencia_estimada": N, "sentimento_predominante": "..."},
    {"nome": "...", "frequencia_estimada": N, "sentimento_predominante": "..."}
  ],
  "insights": "Uma frase descrevendo o principal achado sobre os temas."
}

Dados da amostra:
"""

REPORT_GENERATOR_PROMPT = """\
Você é um especialista em VoC (Voice of Customer) e geração de relatórios
executivos para a Porto Seguro Holding.

Com base nas métricas consolidadas fornecidas em JSON, gere um relatório
executivo completo em Markdown seguindo os padrões Porto Seguro.
Não seja apenas descritivo: seja investigativo, explicando hipóteses, causa
raiz e impacto operacional.

Padrões de escrita:
- Tom formal, objetivo e orientado a dados
- Destacar números e percentuais em negrito
- Usar tabelas Markdown para métricas comparativas
- Linguagem inclusiva e profissional

Diretrizes investigativas obrigatórias:
1) Análise de Contradição
- Se o sentimento dominante for POSITIVO e a fricção for ALTA/CRÍTICA,
  investigue explicitamente a hipótese:
  satisfação com o atendimento humano (educação/cordialidade) versus
  fricção no processo (burocracia, etapas, retrabalho, tempo de resposta).
- Descreva o contraste e o que pode estar mascarando a percepção de qualidade.

2) Causa Raiz por Assunto
- Correlacione os pontos de fricção com os Assuntos/Temas (ex.: Boleto,
  Cartão, Sinistro, etc.).
- Para cada operação analisada, aponte qual assunto atua como principal
  detrator (tema de maior impacto negativo).
- Se não houver granularidade suficiente por operação no JSON de entrada,
  explicite essa limitação e indique o melhor detrator possível com base nos
  temas disponíveis.

3) Cálculo de Desperdício Operacional
- Extrapole os achados da amostra para o volume total de registros.
- Estime o desperdício operacional em horas para:
  a) Rechamadas
  b) Longa Espera
- Mostre a fórmula usada e os pressupostos adotados.

4) Verbatim Estratégico
- Extraia 3 exemplos reais (verbatim) que representem a dor do cliente no
  assunto mais crítico identificado.
- Use somente evidências presentes nos dados recebidos.
- Se o campo de conversa/verbatim não estiver disponível no JSON, informe
  explicitamente "verbatim indisponível nesta execução" e não invente exemplos.

5) Plano de Ação Quick Win
- Separe recomendações em duas frentes:
  a) Ação Imediata (correção de processo)
  b) Ação Estratégica (mudança de régua/produto)
- Priorize ações por impacto esperado e velocidade de implementação.

Estrutura obrigatória do relatório:
1. # Relatório VoC — <período>
2. ## Resumo Executivo
3. ## Distribuição de Sentimento
4. ## Análise de Fricção
5. ## Análise de Contradição (quando aplicável)
6. ## Causa Raiz por Assunto e Detratores por Operação
7. ## Estimativa de Desperdício Operacional
8. ## Verbatim Estratégico
9. ## Plano de Ação Quick Win
10. ---
11. *Índice de qualidade da análise: N/100*

Retorne SOMENTE o JSON abaixo, sem texto adicional:
{
  "markdown_report": "# Relatório VoC ...",
  "quality_score": N
}
"""
