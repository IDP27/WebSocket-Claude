# Postman — APIs da Oracle usadas pelo projeto

Coleção e environment conferidos contra as specs oficiais em `vendor/oracle-hospitality-api-docs/` (ADR-0012). Análise completa: `docs/OHIP_APIS.md`.

## Importar

1. No Postman: **Import** → `ohip-streaming.postman_collection.json` e `ohip-streaming.postman_environment.json`.
2. Selecione o environment **ohip-streaming — sandbox**. Preencha só o **valor atual** (*current value*), que fica na sua máquina: `HostName`, `AppKey`, `CLIENT_ID`, `CLIENT_SECRET` e, conforme o modo do ambiente, `EnterpriseId` + `Scope` (client_credentials) ou `Username` + `Password` (resource owner).
3. Rode **1. OAuth → Token** do modo do seu ambiente (Developer Portal → Environment mostra qual é). O script guarda `Token` e `HashedAppKey`.
4. Preencha `HotelId` e `ReservationId`/`ProfileId` com o `primaryKey` de um evento de teste e rode a pasta **2. Enriquecimento**.

## Regras

- **Só sandbox ou homologação.** Nunca a app key de produção. O Postman não mantém o heartbeat do Streaming, e uma conexão dele disputa o consumidor único (4409, lockout de cerca de 2 min) e derruba o consumer (RUNBOOK).
- **Nunca commite o environment preenchido** nem exporte com *current values*. O arquivo do repositório tem todos os valores vazios, e um teste falha se não tiver. Exportações locais: use o sufixo `.local.json`, que o git ignora.
- **Pedir token é cobrado** para parceiros: reaproveite o token até cerca de 2 min antes do `exp`, em vez de pedir um a cada chamada.
- As respostas de `getProfile` e `getReservation` têm **dados pessoais**: não salve exemplos com dados reais na coleção.
- O Streaming não é testado por aqui. Para explorar o sandbox, abra `vendor/oracle-hospitality-api-docs/graphql/streaming/graphiql.html` no navegador, com a app key do sandbox.

## Coleção oficial completa

A Oracle mantém a coleção completa (2.410 chamadas, 34 módulos, workflows de check-in e checkout) no [workspace público](https://www.postman.com/hospitalityapis/workspace/oracle-hospitality-apis/overview) e em [`postman-collections/`](https://github.com/oracle/hospitality-api-docs/tree/main/postman-collections). Use-a para explorar. Para o que o projeto chama, vale esta coleção.
