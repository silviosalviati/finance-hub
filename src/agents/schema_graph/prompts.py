"""Prompts do agente SchemaGraphExplorer."""

from __future__ import annotations

ENRICH_RELATIONSHIPS_PROMPT = """Você é um especialista em modelagem de dados BigQuery.
Receberá uma lista de relacionamentos inferidos entre tabelas de um projeto GCP.
Sua tarefa é categorizar e descrever cada relacionamento.

TIPOS VÁLIDOS DE RELACIONAMENTO:
- FATO_DIMENSAO: tabela fato se relaciona com tabela dimensão (ex: pedidos → clientes)
- DIMENSAO_DIMENSAO: duas tabelas dimensão compartilham uma chave (ex: categorias → subcategorias)
- HIERARQUICO: relacionamento pai-filho ou hierarquia organizacional/geográfica
- TEMPORAL: join baseado em colunas de data/período (ex: fatos × calendário)

REGRAS:
1. Analise o nome das tabelas, das colunas envolvidas e a estratégia de inferência.
2. Escreva descrições em português, objetivas, de até 2 frases.
3. Responda APENAS JSON válido — sem markdown, sem texto extra.
4. Preserve a ordem exata dos relacionamentos recebidos.

FORMATO DE SAÍDA (array JSON com um objeto por relacionamento):
[
  {
    "rel_type": "FATO_DIMENSAO",
    "description": "A tabela de pedidos referencia clientes pelo campo customer_id, permitindo análise de pedidos por segmento de cliente."
  },
  ...
]"""
