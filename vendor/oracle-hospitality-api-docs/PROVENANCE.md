# Origem dos arquivos

Cópia parcial e **sem alterações** de <https://github.com/oracle/hospitality-api-docs>, sob a Universal Permissive License 1.0 (`LICENSE.txt`). Copyright (c) 2021, 2026 Oracle and/or its affiliates.

- Commit: `4bd129b455bc5e3ab0f900ac47983611e58659b4` (2026-09-16)
- Copiado em: 2026-10-01
- Integridade: `SHA256SUMS` (conferir com `scripts/sync_oracle_api_docs.sh --check`)
- Decisão e processo de atualização: ADR-0012. Análise: `docs/OHIP_APIS.md`.

| Arquivo | Para que serve no projeto |
| --- | --- |
| `graphql/streaming/StreamingGraphQLSchema.json` | Schema oficial do Streaming (introspecção): entrada `NewEventInput` e saída `EventHeader`. Fonte dos testes de contrato. |
| `graphql/streaming/graphiql.html` | Cliente de referência da Oracle (GraphiQL + `graphql-ws`). Mostra URL, hash da app key, `connectionParams` e `keepAlive`. Só leitura; não é servido pelo projeto. |
| `rest-api-specs/security/v1/publishedoauth.json` | `POST /oauth/v1/tokens` (Fase 4). |
| `rest-api-specs/property/v1/int.json` | Business events por polling (alternativa ao Streaming; não usada). |
| `rest-api-specs/property/v1/rsv.json` | Reservas: `getReservation` (enricher, Fase 9). |
| `rest-api-specs/property/v1/crm.json` | Perfis: `getProfile` (enricher, Fase 9). |

Outras specs (ex.: `blk.json`, `fof.json`) entram quando a Q-1 definir os módulos enriquecidos, pelo script de sincronização.
