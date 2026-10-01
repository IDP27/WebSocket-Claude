# APIs da Oracle usadas pelo projeto

Análise das fontes oficiais indicadas em 2026-10-01 e do que entra no projeto. Decisão sobre a cópia local e o processo de atualização: ADR-0012.

| Fonte | Situação |
| --- | --- |
| [oracle/hospitality-api-docs](https://github.com/oracle/hospitality-api-docs) (UPL-1.0) | Analisado no commit `4bd129b` (2026-09-16). Arquivos usados copiados sem alteração para `vendor/oracle-hospitality-api-docs/` |
| [OHIP User Guide — Property APIs](https://docs.oracle.com/en/industries/hospitality/integration-platform/ohipu/c_property_apis.htm) | Lido. Traz as boas práticas de token e de headers (§4) |
| Datasheet `hosp-integration-platform-ds.pdf` (oracle.com) | **Indisponível**: o endereço devolve 404. Nada dele foi usado |

## 1. O que existe no repositório da Oracle

| Pasta | Conteúdo | Uso no projeto |
| --- | --- | --- |
| `graphql/streaming/` | Schema GraphQL do **Streaming** (introspecção) e o cliente de referência `graphiql.html` | **Sim**: contrato do consumer (Fases 2 e 5) e teste de contrato |
| `rest-api-specs/security/v1/publishedoauth.json` | `POST /oauth/v1/tokens` | **Sim**: adapter de token (Fase 4) |
| `rest-api-specs/property/v1/*.json` | 45 specs REST do OPERA Cloud (Swagger 2.0, versão 26.3), uma por módulo: `rsv`, `crm`, `blk`, `fof`, `csh`, `inv`, `hsk`, configuração (`*cfg`) e assíncronas (`*async`) | **Parcial**: `rsv` e `crm` para o enricher (Fase 9). `int` (business events por polling) copiada só como referência. Outras entram com a Q-1 |
| `rest-api-specs/property/outbound/` | Specs de chamadas que o OPERA faz para fora (`crm`, `csh`, `fof` outbound) | Não |
| `rest-api-specs/distribution/`, `nor1/` | Distribuição (canais) e upsell Nor1 | Não |
| `graphql/data-apis/` | 76 schemas das **Data APIs** GraphQL (relatórios e extrações: reservas, perfis, financeiro, estatísticas) | Não. Candidato para reconciliação em massa no futuro (§6) |
| `postman-collections/property/` | Coleção oficial com 2.410 chamadas, de 34 módulos, mais workflows (check-in, checkout etc.) | Não copiada (6,8 MB). O projeto tem uma coleção enxuta (§5); a oficial está no [workspace público da Oracle](https://www.postman.com/hospitalityapis/workspace/oracle-hospitality-apis/overview) |
| `postman-collections/oracle-hospitality.postman_environment.json` | Environment oficial (255 variáveis) | Não. Usamos só as 14 que a coleção do projeto precisa |

## 2. Streaming: o schema oficial comparado com o código

Esta comparação é verificada automaticamente por `tests/contract/test_oracle_contracts.py`.

| Item | Schema oficial | Projeto | Resultado |
| --- | --- | --- | --- |
| Assinatura | `subscription { newEvent(input: NewEventInput!): EventHeader! }` | ADR-0007 | Igual |
| `chainCode` | até 20 caracteres, `^[A-Za-z0-9 _%#$&-]+$` | `CHAIN_CODE_RE` | Igual |
| `hotelCode` | **lista separada por vírgula**, até **50** caracteres no total, `^[A-Za-z0-9 _%#$&,-]+$`; "recomendado só para replay ou depuração, porque perde os eventos de outros hotéis" | `OHIP_HOTEL_CODES` (lista), `HOTEL_CODES_MAX_LEN = 50` | Igual. D-9 vira "confirmado no schema, validar no sandbox" |
| `offset` | scalar `StringWithLength20`, `^[0-9]+$`; a **descrição** do campo diz "max length of 10" | `^[0-9]{1,20}$` (ADR-0006) | Mantemos 20 (vale o scalar); a divergência é a nova **D-11** |
| `offsetType` | só `highest` ("último evento não confirmado"; com ele, o `offset` é ignorado) | Não usamos: sempre enviamos o offset commitado | Compatível |
| `delta` | `Boolean`, padrão `false` | `OHIP_DELTA=false` | Igual |
| `metadata.offset` | `String!` | Aceita texto e número | D-7 resolvida: o schema diz texto; aceitar número continua inofensivo |
| `moduleName`, `eventName`, `primaryKey`, `timestamp` | `String!` | Os três primeiros são obrigatórios no parser; `timestamp` ilegível → `event_ts = null` | Compatível |
| `hotelId`, `publisherId`, `actionInstanceId` | `String` (anuláveis) | Opcionais | Igual |
| `detail[]` | `elementName!`, `oldValue` ("vazio na criação"), `newValue` ("vazio na exclusão"), `scopeFrom`/`scopeTo` (`YYYY-MM-DD`), `elementSequence`, `elementType`, `elementRole` | Todos lidos | Igual |
| `dataValueMapping[]` | **Campo novo**: `{dataElement, operaValue, externalValue}`, opcional | Não lido; não pedido na assinatura | Nova **D-12** |
| `query { connection { id status } }` | `ConnectionStatus { id, status }`, com status `Active` ou `Inactive` | `STATUS_CHECK_ENABLED=false` | Formato confirmado (§3). A D-4 fica só com a validação no sandbox |
| `query { getHelp { ... } }` | Links da documentação | Não usado | — |

## 3. O cliente de referência da Oracle (`graphiql.html`)

O cliente oficial usa a biblioteca `graphql-ws` 5.11 e confirma o ADR-0007:

- URL: `wss://<host>/subscriptions?key=<sha256 hex da app key>`. O cliente troca `https:` por `wss:` e corta `/subscriptions` e a barra final, igual ao nosso `OHIP_GATEWAY_URL`.
- `connectionParams`: `{"Authorization": "Bearer <token>", "x-app-key": "<app key>"}`.
- `keepAlive: 10000`: ping a cada **10 s**. O nosso padrão é 15 s, o máximo do guia. Os dois são válidos; manter 15 s.
- **Queries vão num `subscribe` com `id` próprio** (`next` + `complete`), na mesma conexão: é assim que o GraphiQL roda `getHelp` e `connection`. Isso responde à pergunta de formato da D-4.
- Falha no handshake: o cliente oficial mostra "Unexpected server response: 400 - Incorrect key or URL", ou seja, chave ou URL errada é recusada com HTTP 400 antes do WebSocket abrir. O 4401 aparece como "Invalid Auth Token / API Key". Ação para a Fase 5: tratar o 400 do handshake como erro de configuração (alerta, sem laço de reconexão rápido) e confirmar no sandbox.
- Exemplo do sandbox: `chainCode: "OHIPCN"`, `HotelId` `SAND01` (environment oficial).

## 4. OAuth e chamadas REST

`POST {gateway}/oauth/v1/tokens` (`publishedoauth.json`, versão 26.2):

| Item | Valor oficial | Projeto |
| --- | --- | --- |
| Autenticação | `Authorization: Basic base64(ClientID:ClientSecret)` | `OHIP_CLIENT_ID`, `OHIP_CLIENT_SECRET` |
| Headers | `x-app-key` (obrigatório; o padrão da spec é um UUID v4); `enterpriseId` (`^[A-Z0-9]{1,8}$`, **só** com `client_credentials` em ambientes OCIM) | `OHIP_APP_KEY`, `OHIP_ENTERPRISE_ID` (exigido com `client_credentials`) |
| Corpo | `application/x-www-form-urlencoded`. `grant_type=client_credentials` + `scope`; ou `grant_type=password` + `username` + `password` (usuário de integração do OPERA) | `OHIP_AUTH_MODE` = `client_credentials` ou `resource_owner` (→ `password`) |
| Escopo | `urn:opc:hgbu:ws:__myscopes__` (dois sublinhados; coleção oficial) | Padrão de `OHIP_OAUTH_SCOPE`. A D-2 fica resolvida e o valor continua configurável |
| Resposta | `access_token`, `expires_in` ("normalmente 3600"), `token_type` (`Bearer`), `oracle_tk_context` | Fase 4 |
| Erros | 400 (dados inválidos), 401 (credencial ou escopo), 403 (operação não permitida) | Fase 4: 401/403 → alerta, sem repetição rápida |

Boas práticas do User Guide (Property APIs):

- Renovar o token **2 minutos antes** do `exp` do JWT. O nosso `OHIP_TOKEN_REFRESH_MARGIN_S=300` (5 min) atende.
- **Reaproveitar o token**: pedir token é cobrado para parceiros. O token fica no Redis, compartilhado pelos processos (ARCHITECTURE §7). Referência de tempo: menos de 100 ms por pedido.
- Enviar `X-Request-Id` com um GUID em toda chamada, para a Oracle conseguir investigar. Também vale para o token.
- `X-Originating-Application` só quando um proxy concentra chamadas de vários microsserviços. Não é o nosso caso.

Chamadas do enricher (Fase 9; os módulos dependem da Q-1):

| `moduleName` do evento | Chamada | Spec |
| --- | --- | --- |
| `RESERVATION` | `GET /rsv/v1/hotels/{hotelId}/reservations/{reservationId}` (`getReservation`) | `rsv.json` |
| `PROFILE` | `GET /crm/v1/profiles/{profileId}` (`getProfile`) | `crm.json` |
| `BLOCK` (se a Q-1 incluir) | `GET /blk/v1/hotels/{hotelId}/blocks/{blockId}` (`getBlock`) | `blk.json` (copiar quando necessário) |

Headers obrigatórios em todas: `authorization`, `x-app-key` e **`x-hotelid`**, inclusive no `getProfile`, em que o `hotelId` da query é opcional. Headers opcionais úteis: `x-request-id` e `Accept-Language`. Respostas documentadas: 200, 204, 400, 401, 403, 404, 405, 406, 413, 414, 415, 500, 502 e 503. **O 429 não aparece nas specs**; o tratamento de rate limit (RF-09) continua pelo guia, com `Retry-After`, e entra como teste no servidor simulado.

## 5. Postman no projeto (`postman/`)

- `ohip-streaming.postman_collection.json`: só o que o projeto usa. Token nos dois modos (o script guarda o token e calcula o `HashedAppKey`), `getReservation` e `getProfile`.
- `ohip-streaming.postman_environment.json`: modelo com as 14 variáveis, **todos os valores vazios** e os segredos marcados como `secret`.
- `tests/contract/test_postman_collection.py` garante três coisas: toda chamada da coleção existe na spec oficial com os headers obrigatórios; o environment do repositório não tem valores; nenhuma credencial literal entra na coleção.
- O Streaming **não** é testado pelo Postman. O Postman não mantém o heartbeat, e uma conexão dele disputa o consumidor único (4409). Para explorar o sandbox, use o `graphiql.html` oficial com a app key do sandbox (RUNBOOK).

## 6. O que não usamos, e por quê

| API | Motivo |
| --- | --- |
| Business events por polling (`int`: `GET /int/v1/externalSystem/{code}/hotels/{hotelId}/businessEvents`) | Alternativa ao Streaming para outro modelo de integração. O PRD escolheu o Streaming. A spec fica copiada como referência. Não chamar sem ADR: a spec não deixa claro se a leitura consome a fila de eventos do sistema externo |
| APIs assíncronas (`*async`) | Servem para operações em lote de escrita no OPERA. O projeto só lê |
| Data APIs GraphQL (`graphql/data-apis`) | Extrações em massa. Possível uso futuro: reconciliar tabelas de domínio depois de uma lacuna maior que a retenção de 7 dias (ADR antes) |
| Configuração (`*cfg`), LOV | Tabelas de código do OPERA. Só se uma regra de normalização (Q-1) precisar traduzir códigos |

## 7. Ações para as próximas fases

| Fase | Ação |
| --- | --- |
| 4 (token) | Seguir a tabela do §4: Basic, `x-app-key`, `enterpriseId` só em `client_credentials`, `X-Request-Id`, cache no Redis, renovação ≥ 2 min antes do `exp`, 401/403 sem repetição rápida |
| 5 (WebSocket) | Status query num `subscribe` com `id` próprio (§3); HTTP 400 no handshake = erro de configuração; não pedir `dataValueMapping` até decidir a D-12 |
| 9 (enricher) | Cliente REST com `x-hotelid` sempre; mapa `moduleName → chamada` configurável (§4); copiar `blk.json` ou outras specs pela Q-1 |
| Sempre | Atualizar `vendor/` só pelo script e com os testes de contrato verdes (ADR-0012) |
