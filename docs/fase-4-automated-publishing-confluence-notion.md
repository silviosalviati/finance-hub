# Fase 4 - Automated Publishing (Confluence e Notion)

## Objetivo

Publicar automaticamente a documentacao gerada pelo Document Build em plataformas de consumo do time, reduzindo trabalho manual e garantindo atualizacao continua.

Fluxo alvo de times maduros:

1. Document Build gera documento estruturado em JSON + Markdown.
2. Pipeline de publicacao normaliza formato e metadados.
3. Publicacao automatica envia para Confluence e/ou Notion.
4. Job agenda republicacoes por mudanca de schema, release ou periodicidade.
5. Resultado de publicacao e versionamento ficam auditaveis.

## Arquitetura Recomendada

```text
Document Build (JSON/Markdown)
        |
        v
Publisher Worker (CI/CD ou job agendado)
   |                       |
   v                       v
Confluence API         Notion API
```

Componentes:

- Fonte: saida do agente Document Build.
- Transformador: adapta estrutura para blocos Confluence e blocos Notion.
- Publicador: cliente HTTP com retry, backoff e idempotencia.
- Auditoria: log da execucao, destino, URL publicada e hash de conteudo.

## Modelo de Dados Minimo para Publicacao

Campos recomendados para o payload de publicacao:

- title
- doc_type
- table_path
- markdown_document
- quality_score
- generated_at
- source_agent_version
- source_commit
- content_hash

Regra de idempotencia:

- Se content_hash nao mudou, nao republcar.
- Se mudou, atualizar a pagina existente em vez de criar duplicata.

## Confluence

## Pre-requisitos

- Token de API (usuario tecnico).
- Base URL e Space Key definidos por ambiente.
- Estrategia de pagina pai por dominio (ex.: Dados > Contratos > Dataset).

Variaveis de ambiente sugeridas:

- CONFLUENCE_BASE_URL
- CONFLUENCE_USER_EMAIL
- CONFLUENCE_API_TOKEN
- CONFLUENCE_SPACE_KEY
- CONFLUENCE_PARENT_PAGE_ID

## Fluxo de Publicacao

1. Buscar pagina existente por titulo + ancestor.
2. Se existir: atualizar body.storage com nova versao.
3. Se nao existir: criar pagina no parent definido.
4. Gravar URL retornada e version number em log.

## Recomendacoes de Qualidade

- Sempre incluir macro de metadata (doc_type, table_path, score, ultima atualizacao).
- Inserir mermaid renderizado em fallback textual quando a instancia nao suportar macro.
- Tratar limite de tamanho de pagina e anexos.

## Notion

## Pre-requisitos

- Integration Token.
- Database ID ou Parent Page ID para publicar.
- Permissao da integracao no workspace/pagina alvo.

Variaveis de ambiente sugeridas:

- NOTION_API_TOKEN
- NOTION_DATABASE_ID
- NOTION_PARENT_PAGE_ID

## Fluxo de Publicacao

1. Buscar pagina por propriedade unica (table_path + doc_type).
2. Upsert dos blocos com base no markdown convertido.
3. Atualizar propriedades operacionais:

- Quality Score
- Source Commit
- Updated At
- Status (Published/Failed)

## Recomendacoes de Qualidade

- Definir schema de propriedades padrao no database Notion.
- Limitar tamanho por bloco e paginar conteudo longo.
- Incluir backlink para logs de pipeline.

## Estratégia de Orquestracao

## CI/CD (recomendado para maturidade)

Acionadores:

- Merge em main alterando src/agents/document_build ou docs/contracts.
- Execucao noturna para refresh de documentacao critica.
- Evento de schema drift detectado.

Etapas:

1. Gerar documento.
2. Validar contrato minimo (title, table_path, markdown_document).
3. Publicar nos destinos habilitados.
4. Notificar resultado (Teams/Slack).

## Job Agendado (alternativa inicial)

- Executar por cron em ambiente de runtime.
- Persistir ultimo hash publicado por destino.

## Politicas Operacionais

- Retry com backoff exponencial para 429/5xx.
- Circuit breaker por destino para evitar cascata de falhas.
- Timeout curto por request e total timeout por run.
- Dead-letter para payloads invalidos.

## Governanca e Seguranca

- Secret manager para tokens (nao usar .env em producao).
- Publicacao com conta tecnica dedicada e escopo minimo.
- PII redaction antes de publicar fora de ambientes restritos.
- Trilha de auditoria com:

- quem publicou
- quando publicou
- qual conteudo/hashes
- para qual URL

## Definition of Done - Fase 4

1. Confluence e/ou Notion com upsert idempotente.
2. Hash de conteudo evitando publicacao redundante.
3. Logs de auditoria por documento.
4. Alarmes para erro de publicacao e token expirado.
5. Processo de rollback (restaurar versao anterior).

## Checklist de Implantacao

- [ ] Variaveis de ambiente de Confluence configuradas.
- [ ] Variaveis de ambiente de Notion configuradas.
- [ ] Tabela/arquivo de controle de content_hash implementado.
- [ ] Job no CI/CD ou scheduler ativo.
- [ ] Alertas e dashboard de publicacao ativos.
- [ ] Politica de segredo e rotacao de token documentada.
