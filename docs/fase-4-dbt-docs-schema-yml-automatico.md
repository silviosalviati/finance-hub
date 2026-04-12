# Fase 4 - dbt docs com geracao automatica de schema.yml

## Objetivo

Gerar automaticamente arquivos schema.yml a partir dos artefatos reais (BigQuery schema, Dataplex e Document Build), reduzindo manutencao manual e aumentando cobertura de documentacao para dbt docs.

Fluxo alvo:

1. Ler schema real da tabela no BigQuery.
2. Enriquecer descricao de modelo/colunas com Document Build + Dataplex + manifest dbt.
3. Materializar schema.yml por modelo.
4. Executar dbt docs generate.
5. Publicar dbt docs em ambiente interno.

## Estrutura Sugerida

```text
docs/
  dbt/
    schema/
      <model>.yml
scripts/
  generate_schema_yml.py
```

## Contrato Minimo do schema.yml

Campos por modelo:

- version
- models[].name
- models[].description
- models[].columns[].name
- models[].columns[].description

Campos recomendados:

- tags
- meta.owner
- meta.table_path
- tests (unique, not_null, accepted_values, relationships)

## Exemplo de schema.yml gerado

```yaml
version: 2
models:
  - name: fatos_vendas
    description: "Fato de vendas consolidado para analise comercial e financeira."
    meta:
      owner: "dados@empresa.com"
      table_path: "projeto.dataset.fatos_vendas"
      source: "document_build"
    columns:
      - name: transacao_id
        description: "Identificador unico da transacao."
        tests:
          - not_null
          - unique
      - name: cliente_id
        description: "Identificador do cliente para relacao com dimensoes."
        tests:
          - not_null
```

## Regras de Geracao Automatica

Prioridade de fonte para descricoes:

1. manifest.json do dbt (quando ja existir descricao manual)
2. Dataplex/Data Catalog (tags e glossario)
3. Document Build (texto enriquecido)
4. fallback: descricao padrao por tipo de coluna

Regras de testes automaticos (heuristicas iniciais):

- colunas terminadas em \_id: not_null e candidate para unique/relationships
- colunas REQUIRED no BigQuery: not_null
- colunas categóricas pequenas: accepted_values (se dominio conhecido)
- campos de timestamp/data: teste de recencia em camada de observabilidade

## Pipeline de Geracao

## Passo 1 - Descoberta de modelos

- Ler manifest.json e identificar nodes model.
- Resolver mapeamento model -> table_path.

## Passo 2 - Coleta de metadados

- BigQuery: tipo, mode, particionamento, clustering.
- Dataplex: tags/aspects e glossario.
- Document Build: objetivo, riscos, regras, checklist.

## Passo 3 - Montagem do YAML

- Gerar estrutura version: 2.
- Incluir colunas existentes no schema real.
- Preservar manual edits quando configurado (merge inteligente).

## Passo 4 - Validacao

- Validar YAML parse.
- Executar dbt parse.
- Falhar pipeline se arquivo invalido.

## Passo 5 - Publicacao

- Commit automatizado dos arquivos gerados (branch de bot ou PR).
- Executar dbt docs generate.
- Publicar artefato html/json.

## Merge Inteligente (evitar sobrescrever curadoria humana)

Estrategia:

- Se descricao manual existe no schema.yml atual, preservar.
- Se descricao vazia, preencher automaticamente.
- Se coluna removida do schema real, marcar como deprecated e abrir alerta.
- Se coluna nova, adicionar automaticamente com status pending_review.

## Qualidade e Governanca

- Exigir owner em meta para cada modelo.
- Registrar source_commit e generated_at em meta.
- Versionar alteracoes em PR com diff legivel.
- Auditar cobertura de documentacao por modelo (% colunas com descricao).

## KPIs da Fase 4

- Cobertura de descricao de colunas >= 90%.
- Tempo medio de atualizacao de docs < 1 dia apos schema change.
- Taxa de erro de dbt parse por schema.yml gerado = 0.
- Reuso de descricoes curadas (sem overwrite indevido) >= 95%.

## Script de Referencia (pseudo fluxo)

```python
# 1) carregar manifest
# 2) para cada modelo: obter table_path
# 3) buscar schema real no BigQuery
# 4) buscar contexto Dataplex
# 5) buscar contexto Document Build
# 6) gerar/merge schema.yml
# 7) validar YAML e rodar dbt parse
# 8) salvar arquivos e reportar diff
```

## Checklist de Implantacao

- [ ] Script de geracao de schema.yml implementado.
- [ ] Merge inteligente com preservacao de curadoria manual.
- [ ] Validacao automatica com dbt parse no CI.
- [ ] Gatilho por mudanca de schema/manifest.
- [ ] Publicacao de dbt docs automatizada.
- [ ] Dashboard de cobertura de documentacao ativo.
