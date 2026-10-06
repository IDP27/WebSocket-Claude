# Q-1 — Escopo de eventos e hotéis (pacote de decisão)

> Para quem decide o negócio. O que está aqui destrava a Fase 9b (regras do enricher e tabelas de domínio), que o projeto não pode começar sem resposta (CLAUDE.md).

## 1. O que está parado e por quê

O consumer já grava **todos** os eventos que a app recebe (`OHIP_EVENT_RAW`) e os publica no RabbitMQ. O enricher recebe a fila `ohip.enricher`, mas não tem regras. Com `ENRICHER_BINDINGS` vazio (padrão), só reprocessamentos chegam a ele. Para medir o volume por evento antes de decidir (ajuda nas perguntas 1 e 8), ligue `ENRICHER_BINDINGS=ohip.#` num ambiente de teste: tudo termina `UNMAPPED` e a métrica `ohip_events_unmapped_total` conta por `event_name`. Para escrever as regras e as tabelas de domínio, é preciso saber **quais eventos importam e que informação o negócio quer tirar deles**.

## 2. O que a documentação oficial diz (e o que não diz)

- **Não há catálogo de eventos** no schema do Streaming (`vendor/oracle-hospitality-api-docs/graphql/streaming/StreamingGraphQLSchema.json`). Cada evento traz `moduleName`, `eventName`, `primaryKey`, `hotelId`, `timestamp` e `detail` (lista de `elementName`/`oldValue`/`newValue`), mas os nomes possíveis não estão lá.
- Os eventos são **escolhidos e aprovados no Developer Portal** do ambiente; o `subscribe` não filtra por evento (DV-13). A lista válida é a que aparece no Portal da app — é dali que a resposta deve sair.
- Os exemplos da API de Business Events por polling (`rest-api-specs/property/v1/int.json`, que **não** usamos) mostram `NEW RESERVATION`, `UPDATE RESERVATION`, `NEW PROFILE`, `UPDATE PROFILE` e o elemento `RESERVATION STATUS` mudando para `CHECKED IN`. Servem de indício do formato, não de contrato do Streaming.
- O padrão provisório da Q-1 (NEW/UPDATE/CANCEL RESERVATION, CHECK IN, CHECK OUT) vem do PRD. **Os nomes exatos precisam ser conferidos no Portal** antes de virar regra.

## 3. Perguntas a responder

Preencha a coluna "Resposta". "Padrão" é o que o código faz hoje; "proposta" é sugestão do projeto, ainda não decidida.

| # | Pergunta | Por que importa | Padrão ou proposta | Resposta |
| --- | --- | --- | --- | --- |
| 1 | Quais eventos (`moduleName` + `eventName`, como aparecem no Portal)? | Uma regra por evento; o resto continua `UNMAPPED` (gravado no bruto, sem tabela) | Proposta do PRD: reservas (novo, alteração, cancelamento, check-in, check-out) | |
| 2 | Quais hotéis? Todos os aprovados na app ou uma lista? | `OHIP_HOTEL_CODES` (lista por vírgula, até 50 caracteres; o schema recomenda só para replay, D-9) | Padrão: todos os aprovados (vazio) | |
| 3 | Quantas chains na fase 1? | App de cliente vale para **uma** chain (Q-4); cada chain é um consumer | Padrão: uma (Q-4) | |
| 4 | Para cada evento: que informação o negócio precisa (ex.: datas, status, quarto, tarifa, hóspede)? | Define as colunas das tabelas de domínio e se o `detail` do evento basta | — | |
| 5 | O `detail` basta ou é preciso buscar o recurso completo na REST (`getReservation`, `getProfile`)? | Liga `ENRICHER_REST_ENABLED`; consome limite de requisições do OHIP; latência (Q-2) | Padrão: não buscar (`ENRICHER_REST_ENABLED=false`) | |
| 6 | Guardar só o **estado atual** de cada reserva/perfil ou também o **histórico** de mudanças? | Uma linha por recurso (MERGE condicional, já pronto) ou uma por evento | Proposta: estado atual (o histórico já fica no bruto pelo prazo de retenção, Q-9) | |
| 7 | Quais dados pessoais podem ir para as tabelas de domínio e por quanto tempo? | LGPD (Q-9); máscara e retenção | — (a Q-9 hoje cobre só o bruto e as saídas) | |
| 8 | Quem consome as tabelas de domínio (BI, n8n, outro sistema) e com que atraso aceitável? | Índices, grants (`OHIP_READ`) e meta de latência (Q-2) | — | |
| 9 | Traduzir códigos OPERA para códigos externos (`dataValueMapping`)? | Pedir o campo na assinatura muda o contrato v1 (D-12, exige ADR) | Padrão: não pedir | |
| 10 | Se houver eventos de perfil: perfis compartilhados (sem `hotelId`) entram? | A REST exige `x-hotelid` (D-13) | Padrão: recusa → DLQ `ENRICH` | |

## 4. O que acontece depois da resposta (Fase 9b)

1. Registrar a resposta na Q-1 (`docs/OPEN_QUESTIONS.md`) e conferir os nomes dos eventos no Portal.
2. **Arquiteto:** ADR das tabelas de domínio (DDL compatível com 11g: `SEQUENCE`, sem `IDENTITY`, identificadores ≤ 30) e das regras; ajuste do `OHIP_EVENT_ALLOWLIST`, `ENRICHER_BINDINGS` e `OHIP_MODULE_CODES`.
3. **DBA:** aplica o novo `sql/004_*.sql` (o projeto não roda DDL em banco real).
4. **Engenheiro:** uma `NormalizationRule` por evento em `build_rules()` (`src/ohip_streaming/entrypoints/enricher.py`), com testes; se a resposta 5 for "sim", copiar as specs REST que faltarem pelo `scripts/sync_oracle_api_docs.sh` (ADR-0012).
5. Revisor → aprovação humana → deploy.

O resto do pipeline (consumer, outbox, publisher, enricher com MERGE condicional, DLQ, API e painel) já está pronto e não muda.
