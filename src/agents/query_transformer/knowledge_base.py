"""Base de boas práticas Dataform/SQLX para o Query Transformer.

Não existe integração com um repositório Dataform real no finance-hub (por
decisão de escopo — ver `docs/plans/2026-08-23-query-transformer-design.md`),
então este módulo não recupera nada de um projeto real: é uma base curada
de trechos de convenção (config(), incremental, assertions, ref()/source()),
indexada uma vez por processo e consultada por similaridade de cosseno em
memória — mesmo padrão simples do `catalog_index.py` do Finance Auditor
(`src/agents/finance_auditor/catalog_index.py`), sem vector DB dedicado
porque o corpus é pequeno.
"""

from __future__ import annotations

from src.shared.tools.embeddings import cosine_similarity, get_embeddings

_SNIPPETS: list[str] = [
    "config(): todo arquivo .sqlx começa com um bloco config { type: ... } "
    "que declara o tipo de materialização e metadados — nunca escreva SQL "
    "solta sem esse bloco.",

    "type: \"table\": recria a tabela inteira a cada execução. Use como "
    "padrão para transformações que processam o histórico completo e não "
    "são grandes o suficiente para justificar incremental.",

    "type: \"view\": não materializa dados, só salva a definição da query. "
    "Use para transformações leves (filtros, joins simples, renomeação de "
    "colunas) que não valem o custo de armazenamento de uma tabela.",

    "type: \"incremental\": processa só as linhas novas desde a última "
    "execução, usando a cláusula `when(incremental())` para filtrar por "
    "uma coluna de data/timestamp. Ideal para tabelas de fatos grandes que "
    "crescem por período (ex.: eventos, pedidos, logs) — evita reprocessar "
    "todo o histórico a cada rodada.",

    "uniqueKey em incremental: quando o modo de atualização precisa "
    "sobrescrever linhas existentes (merge) em vez de só inserir, declare "
    "`uniqueKey: [\"coluna_id\"]` no config() para o Dataform saber qual "
    "coluna identifica uma linha única.",

    "bigquery.partitionBy: dentro do config(), o bloco "
    "`bigquery: { partitionBy: \"campo_data\" }` preserva o particionamento "
    "físico da tabela de destino — essencial para manter a performance e o "
    "custo baixo quando a tabela de origem já era particionada.",

    "bigquery.clusterBy: `bigquery: { clusterBy: [\"campo1\", \"campo2\"] }` "
    "preserva o clustering físico da tabela de destino, mesmo raciocínio do "
    "partitionBy.",

    "assertions — uniqueKey: `assertions: { uniqueKey: [\"id\"] }` no "
    "config() declara um teste automático de que a coluna (ou combinação de "
    "colunas) é de fato única no resultado — vira uma checagem de "
    "qualidade que roda a cada execução do Dataform.",

    "assertions — nonNull: `assertions: { nonNull: [\"coluna_critica\"] }` "
    "declara colunas que nunca devem ser NULL no resultado — use em chaves "
    "e métricas que não fazem sentido ausentes.",

    "ref() vs source(): use `${ref(\"nome_do_modelo\")}` para referenciar "
    "outro modelo Dataform (uma tabela que o próprio pipeline já "
    "transforma) — isso constrói o grafo de dependências automaticamente. "
    "Use `${source(\"dataset\", \"tabela\")}` para tabelas de origem "
    "externas ao pipeline (dados brutos replicados de um sistema "
    "operacional, por exemplo).",

    "Nunca referencie uma tabela por nome totalmente qualificado "
    "(`projeto.dataset.tabela`) dentro do corpo de um .sqlx — isso quebra o "
    "grafo de dependências do Dataform e impede rastrear lineage. Sempre "
    "use ref()/source().",

    "tags: o config() pode incluir `tags: [\"dominio\", \"time\"]` para "
    "organizar e filtrar modelos por área de negócio ou responsável, "
    "facilitando execuções seletivas (`dataform run --tags`).",

    "description e columns: um config() completo documenta o modelo com "
    "`description: \"...\"` e um bloco `columns: { coluna: \"descrição\" }` "
    "por campo relevante — isso vira a documentação viva do catálogo de "
    "dados gerado pelo Dataform.",

    "Evite SELECT * em modelos Dataform: liste as colunas explicitamente "
    "no corpo da query — protege o modelo de quebras silenciosas quando a "
    "tabela de origem ganha ou perde colunas.",

    "Custo e performance: prefira agregações em uma única leitura da tabela "
    "principal (single scan) em vez de múltiplos JOINs/CTEs evitáveis — o "
    "mesmo princípio de performance de SQL manual vale dentro de um modelo "
    "Dataform.",
]

_embeddings_cache: list[tuple[str, list[float]]] | None = None


def _get_indexed_snippets() -> list[tuple[str, list[float]]]:
    global _embeddings_cache
    if _embeddings_cache is None:
        embedder = get_embeddings()
        vectors = embedder.embed_documents(_SNIPPETS)
        _embeddings_cache = list(zip(_SNIPPETS, vectors))
    return _embeddings_cache


def retrieve_best_practices(query_context: str, top_k: int = 4) -> list[str]:
    """Devolve os `top_k` trechos de boas práticas mais relevantes para o
    contexto (a SQL original + metadados já inferidos), por similaridade de
    cosseno contra os embeddings pré-computados do corpus curado acima.
    """
    try:
        indexed = _get_indexed_snippets()
        embedder = get_embeddings()
        query_vector = embedder.embed_query(query_context)
        scored = [
            (cosine_similarity(query_vector, vector), snippet)
            for snippet, vector in indexed
        ]
        scored.sort(key=lambda item: item[0], reverse=True)
        return [snippet for _, snippet in scored[:top_k]]
    except Exception:
        # RAG é um enriquecimento, não uma dependência dura — se os
        # embeddings falharem (ex.: sem credenciais no ambiente), o agente
        # segue com o conhecimento geral do LLM sobre Dataform.
        return _SNIPPETS[:top_k]


__all__ = ["retrieve_best_practices"]
