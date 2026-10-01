# ADR-0012 — Especificações oficiais da Oracle no repositório

- Status: Proposto
- Data: 2026-10-01

## Contexto

A regra "não invente contratos da Oracle" dependia de documentação dispersa (guia em HTML, exemplos divergentes). A Oracle publica no GitHub ([oracle/hospitality-api-docs](https://github.com/oracle/hospitality-api-docs), licença UPL-1.0) o schema GraphQL do Streaming, as specs REST e as coleções Postman. Essas fontes resolvem pontos que estavam em aberto: D-2 (OAuth), D-4 (formato da status query), D-7 (tipo do offset) e, em parte, D-9 (lista de hotéis). Também revelam divergências novas: D-11 (tamanho do offset) e D-12 (`dataValueMapping`).

## Decisão

1. **Cópia parcial e fixada** em `vendor/oracle-hospitality-api-docs/`, sem alterações, com `LICENSE.txt`, `PROVENANCE.md` (commit e finalidade de cada arquivo) e `SHA256SUMS`. Entram só os arquivos que o projeto usa: schema e cliente de referência do Streaming, OAuth, `rsv`, `crm` e `int`. Outras specs entram quando uma fase precisar delas (ex.: `blk` pela Q-1).
2. **Atualização só pelo `scripts/sync_oracle_api_docs.sh <commit>`**, que baixa o commit indicado, copia a lista fixa de arquivos e regrava os checksums. `--check` confere a integridade. Sem dependência nova (curl, tar e shasum).
3. **Testes de contrato** (`tests/contract/`), rodando no `make check`:
   - os validadores de `chainCode`, `hotelCode` e `offset` aceitam exatamente o que o schema aceita;
   - todo campo lido pelo parser existe no schema, e os obrigatórios são `!`;
   - um campo novo no `EventHeader` falha o teste até ser decidido;
   - o caminho, o corpo e os headers do OAuth batem com a config;
   - toda chamada da coleção Postman existe na spec, com os headers obrigatórios.
4. **Em conflito, vale o schema/spec** sobre exemplos do guia. Se o próprio schema se contradiz (D-11), vale a opção mais permissiva e segura, e a dúvida fica registrada.
5. **Postman do projeto** em `postman/`: coleção enxuta das chamadas usadas e um environment-modelo com todos os valores vazios (há teste). A coleção oficial completa (6,8 MB) não é copiada; fica o link para o workspace da Oracle.

## Consequências

- Atualizar a cópia vira uma revisão explícita: o diff mostra o que a Oracle mudou e os testes de contrato apontam o impacto. Mudança de contrato continua exigindo ADR (RNF-15).
- O repositório cresce cerca de 3,6 MB (`rsv.json` e `crm.json` são a maior parte).
- O schema copiado é o oficial publicado, não necessariamente o do gateway do cliente. A validação no sandbox (D-4, D-9, D-10, D-11) continua necessária.
