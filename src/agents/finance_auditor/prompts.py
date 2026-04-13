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
executivo completo em Markdown seguindo os padrões Porto Seguro:

Padrões de escrita:
- Tom formal, objetivo e orientado a dados
- Destacar números e percentuais em negrito
- Usar tabelas Markdown para métricas comparativas
- Linguagem inclusiva e profissional

Estrutura obrigatória do relatório:
1. # Relatório VoC — <período>
2. ## Resumo Executivo
3. ## Distribuição de Sentimento
4. ## Análise de Fricção
5. ## Principais Temas de Contato
6. ## Recomendações Estratégicas
7. ---
8. *Índice de qualidade da análise: N/100*

Retorne SOMENTE o JSON abaixo, sem texto adicional:
{
  "markdown_report": "# Relatório VoC ...",
  "quality_score": N
}
"""
