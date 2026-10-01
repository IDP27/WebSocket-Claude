# Arquitetura — Consumidor de Eventos OHIP Streaming

> Status: **Fase 0 aprovada em 2026-10-01.** Fontes: PRD v0.1, *OHIP Streaming Implementation Guide* G52431-02 (16/07/2026) e schema GraphQL oficial (`oracle/hospitality-api-docs`, `graphql/streaming/StreamingGraphQLSchema.json`).
> Itens marcados `TODO(confirmar-doc)` dependem de confirmação; estão em [OPEN_QUESTIONS.md](OPEN_QUESTIONS.md) como `D-n`. Divergências do PRD estão lá como `DV-n`.

## 1. Visão geral

```
                   ┌───────────────────────── VM Linux (systemd) ──────────────────────────┐
OPERA Cloud        │                                                                        │
    │ eventos      │  ┌──────────────┐  1 transação   ┌──────────────────────────────┐      │
    ▼              │  │ ohip-consumer│ ─────────────▶ │ Oracle 11g                   │      │
OHIP Gateway ─wss──┼─▶│ (1 por chain)│  (lease+epoch) │ EVENT_RAW + OFFSET + OUTBOX  │      │
    ▲              │  └──────────────┘                │ LEASE · DLQ · STATUS · REPLAY│      │
    │ REST GET     │                                  └──────┬────────────────▲──────┘      │
    │              │      Redis ◀──── token, caches,         │ lê PENDING     │ MERGE       │
    │              │                  métricas               ▼                │ condicional │
    │              │                               ┌───────────────┐   ┌──────┴───────┐     │
    └──────────────┼───────────────────────────────│ ohip-enricher │◀──│ RabbitMQ     │◀─┐  │
                   │                               │ (N)           │   │ ohip.events  │  │  │
                   │                               └───────────────┘   └──────┬───────┘  │  │
                   │  ┌───────────────┐                                       │   ┌──────┴─────────┐
                   │  │ ohip-api      │◀── Nginx (TLS+auth) ◀── ohip-admin    │   │ ohip-publisher │
                   │  │ FastAPI       │                        (Flask)        │   │ (1, lease)     │
                   │  └───────────────┘                                       │   └────────────────┘
                   └──────────────────────────────────────────────────────────┼─────────────────────┘
                                                                              ▼
                                                                    n8n (RabbitMQ Trigger)
```

Princípios:

1. **Oracle é a fonte da verdade**, inclusive da liderança. Evento bruto, offset e outbox nascem na mesma transação; a trava de instância única é um *lease* com epoch no Oracle (ADR-0008). Redis só acelera (token, caches, métricas); sem ele o sistema fica mais lento, nunca incorreto nem parado.
2. **Um único caminho de escrita por chain.** Um `ohip-consumer` por chain, gravação sequencial, preservando a ordem garantida pelo OHIP por (appKey, chainCode, gateway).
3. **Nenhum evento é descartado no caminho de entrada.** Ou ele é commitado (como evento, duplicado ou item de DLQ) e o offset avança, ou o offset não avança e o OHIP reenvia.
4. **Nada lento no laço do WebSocket.** Leitura só enfileira; gravação em outra tarefa; publicação e enriquecimento desacoplados pela outbox.
5. **Contratos explícitos e versionados**: mensagem da fila (`schema_version`), API (`/api/v1`) e DDL numerada.

## 2. Processos

| Serviço systemd | Tecnologia | Responsabilidade | Instâncias |
| --- | --- | --- | --- |
| `ohip-consumer@<chain>` | asyncio + `websockets`; executor Oracle de 1 thread (escrita) + 1 conexão de controle | Protocolo OHIP, heartbeat, token, gravação em micro-lotes, DLQ `CONSUME`, status, lease, replay | **1 por chain** (template systemd `@`) |
| `ohip-publisher` | asyncio + `aio-pika` | Outbox → RabbitMQ com publisher confirms, ordem por chain, backoff, `FAILED` + DLQ | 1 (lease `publisher`) |
| `ohip-enricher` | asyncio + `aio-pika` + `httpx` | Consome fila, lê o bruto no Oracle, cache, REST do OHIP, `MERGE` condicional | N |
| `ohip-api` | FastAPI + Uvicorn | Saúde, métricas, consulta, comandos administrativos | 1+ workers |
| `ohip-admin` | Flask + Gunicorn + Jinja2 (+HTMX) | Painel operacional; **só fala com a `ohip-api`** | 1+ workers |
| Nginx | — | TLS, autenticação, roteamento `/api` e `/admin` | 1 |

O consumer **nunca** roda dentro de Uvicorn/Gunicorn (ADR-0001). A comunicação consumer ↔ API é pelo Oracle (`OHIP_CONSUMER_STATUS`, `OHIP_OFFSET`, `OHIP_REPLAY_REQUEST`). Units systemd com `RestartSec=10` (regra dos 10 s, ADR-0007).

## 3. Protocolo OHIP

| Item | Valor | Fonte |
| --- | --- | --- |
| URL | `wss://<gateway>/subscriptions?key=<sha256(appKey) em hex minúsculo>` | Guia: Subscribing; Prerequisites → Hashing |
| Subprotocolo | `Sec-WebSocket-Protocol: graphql-transport-ws` (sem ele: 4406) | Guia: Subscribing; Troubleshooting |
| `connection_init` | em até **5 s** (senão 4408); payload `{"Authorization": "Bearer <token>", "x-app-key": "<appKey>"}` | Guia: Subscribing |
| `subscribe` | `id` = GUID novo por assinatura, gerado após `connection_init`, reutilizado no `complete` | Guia: ID Lifecycle |
| Input `newEvent` | `chainCode`; `offset` (string `^[0-9]+$`, tipo `StringWithLength20`); `offsetType` (`highest`); `delta` (bool); `hotelCode` (descrito no schema como lista separada por vírgula; o guia mostra só um código — D-9) | Schema GraphQL; Guia: Examples of the Subscribe Call |
| Campos de saída | `metadata{offset uniqueEventId}`, `moduleName`, `eventName`, `primaryKey`, `timestamp`, `hotelId`, `publisherId`, `actionInstanceId`, `detail{elementName oldValue newValue scopeFrom scopeTo ...}` | Schema GraphQL; Guia: Interpreting the Event |
| Escolha de eventos | Feita no **Developer Portal** (assinatura + aprovação do dono do ambiente), não no `subscribe` | Guia: Prerequisites → Application Key |
| Heartbeat | cliente envia `{"type":"ping"}` a cada **15 s** e responde `pong` aos pings do servidor; servidor fecha se não vê `pong` em **180 s** | Guia: Performance Considerations |
| Backpressure | acima de ~1,8 MB o servidor envia em rajadas e pode **adiar o `pong`**; mensagens `next` contam como prova de vida | Guia: Backpressure Mode |
| Encerramento | `complete` com o mesmo `id`, **processar o que chegar e esperar o servidor fechar** (não fechar do cliente); fechamento normal = 1000 | Guia: Subscribing; Troubleshooting |
| Intervalo | **≥ 10 s** entre `complete`/desconexão e o próximo `subscribe`; se o horário da última desconexão for desconhecido (crash/restart), **sempre esperar 10 s** | Guia: Limitations; Scaling |
| Retenção | **7 dias**; após 24 h desconectado, o `offset` é obrigatório | Guia: Limitations; FAQ |
| Entrega | pelo menos uma vez, **estritamente ordenada** por (appKey, chainCode, gateway); sem ACK; sem DLQ no servidor | Guia: Limitations |
| Duplicados | "mesmo `primaryKey` e `offset`" = duplicado; processar o primeiro | Guia: Troubleshooting |
| Consumidor único | um assinante por (appKey, chainCode, gateway); segundo recebe **4409**; lockout ≈ **2 min** + jitter | Guia: Limitations; Troubleshooting |
| Limite de requisições | Streaming não é limitado por vazão, mas "requisições de entrada" têm limite de 12/min (rajada 100/min) — D-5 | Guia: Scaling |
| Token | OAuth `POST <gateway>/oauth/v1/tokens`; **token por chain/ambiente**; renovar antes do `exp`. O guia não cobre a obtenção do token: `TODO(confirmar-doc)` D-2 | Guia: Prerequisites → OAuth Token; OHIP User Guide |
| Status da conexão | `query { connection { id status } }` após init; `Inactive` → pode assinar. Formato no graphql-transport-ws a validar (D-4); **desligado por padrão** | Guia: Scaling Recommendations |

### 3.1 Máquina de estados do consumer

```
          ┌────────────┐  lease Oracle (epoch++)   ┌────────────┐
 start ──▶│ ACQUIRING  │──────────────────────────▶│  WAITING   │ espera ≥10 s desde last_disconnect_at
          └────────────┘  (ocupado: tenta de novo) └─────┬──────┘ (desconhecido → 10 s fixos)
                ▲                                        ▼
                │                                  ┌────────────┐ wss + subprotocolo
                │                                  │ CONNECTING │
                │                                  └─────┬──────┘ connection_init (≤5 s)
                │                                        ▼
                │                                  ┌────────────┐  connection_ack
                │                                  │  INIT_SENT │─────────────┐
                │                                  └────────────┘             ▼
                │                                                 ┌───────────────────────┐
                │                                                 │ STATUS_CHECK (opcional)│
                │                                                 └──────────┬────────────┘
                │   4401: novo token                                         ▼
                │   4409: 120 s + jitter                           ┌─────────────────┐
                │   4504: 15 s                                     │   SUBSCRIBED    │◀── next → fila interna
                │   rede: backoff 10 s→60 s                        │ ping 15 s       │
                └────────────── WAITING ◀─────────────────────────┴───┬─────────┬───┘
                                   ▲                                   │         │ token perto do exp,
                                   │ servidor fecha (1000) ou timeout  │         │ replay, fila travada,
                                   └──────────────────── DRAINING ◀────┘         │ Oracle fora, SIGTERM
                                                        (complete enviado) ◀─────┘
 close 4403 / 4406, ou mensagem grande demais N vezes ──▶ STOPPED (alerta; exige ação humana)
```

- `last_disconnect_at` é persistido em `OHIP_CONSUMER_STATUS` **com o relógio do banco** a cada `complete`/fechamento; a espera é calculada em SQL. Ao iniciar, se o último estado gravado não for `WAITING`/`STOPPED` (houve crash, horário real da desconexão desconhecido), espera 10 s fixos a partir de agora. `last_disconnect_at` nulo (primeira partida) também conta como desconhecido: 10 s fixos (ADR-0007).
- Antes de todo `subscribe`, o consumer aplica um replay `PENDING`, se houver (§4.6), e assina com o último offset persistido, se existir.

## 4. Fluxo de dados

### 4.1 Entrada (consumer)

1. OHIP entrega uma mensagem `next` na assinatura da chain.
2. **Laço de leitura** (`ohip_ws`): valida o envelope (`id`, `type`), marca prova de vida e coloca a mensagem bruta numa fila interna limitada por quantidade **e** por bytes (`INTAKE_QUEUE_MAX`, padrão 5.000; `INTAKE_QUEUE_MAX_BYTES`, padrão 64 MiB). Nunca faz I/O de banco.
   - **Fila cheia**: o laço **espera** espaço (`await queue.put`) e para de ler o socket. **Nunca descarta.** O heartbeat (envio de `ping`) segue em outra tarefa; o servidor tolera 180 s sem `pong`.
   - Se a fila ficar cheia por mais de `INTAKE_STALL_TIMEOUT` (padrão 120 s), ou se o Oracle estiver fora (item 5), a conexão é marcada como **envenenada** (`poisoned`):
     1. o `put` pendente é cancelado e o que estiver na fila e no lote não commitado é descartado;
     2. o escritor para: **nada mais recebido nesta conexão é gravado**, nem mesmo o que chegar durante a drenagem (sem isso, uma mensagem posterior moveria o offset por cima das descartadas);
     3. envia `complete`, ignora as mensagens seguintes e espera o servidor fechar (com `DRAIN_TIMEOUT`);
     4. reconecta pelo último offset **commitado**. O OHIP reenvia o que faltou.

     Cenário obrigatório no servidor simulado (Fase 5): fila cheia + mensagens chegando durante a drenagem → nenhum offset pulado.
3. **Tarefa de gravação** (uma por consumer): junta um micro-lote (até `BATCH_MAX_EVENTS`, padrão 200, ou `BATCH_MAX_WAIT_MS`, padrão 200 ms), converte cada mensagem no domínio (`Event`) e chama `ProcessEventBatch` no executor Oracle de 1 thread. Mensagens que o domínio rejeita (JSON inválido, sem `uniqueEventId`, offset fora de `^[0-9]{1,20}$`, campos acima dos limites da DDL, campos obrigatórios nulos) viram itens de DLQ `CONSUME` **no mesmo lote**.
4. **Transação do lote** (ADR-0008, ADR-0009):
   1. `INSERT` em lote em `OHIP_EVENT_RAW` com `executemany(..., batcherrors=True)`.
      - ORA-00001 em `ohip_event_raw_uq_evt` (mesmo `uniqueEventId`) ou em `ohip_event_raw_uq_off` (mesmos chain, offset e `primaryKey`) = **duplicado**: ignorado, métrica + log.
      - **Qualquer outro erro por linha** → item de DLQ `CONSUME` com a mensagem bruta, na mesma transação.
   2. `INSERT` em `OHIP_OUTBOX` (`PENDING`) só para os eventos novos e permitidos (`OHIP_EVENT_ALLOWLIST`, vazio = todos; fora da lista → `processing_status = IGNORED`, sem outbox — DV-13).
   3. `UPDATE OHIP_OFFSET` com o offset da **última mensagem do lote que tinha offset válido** (gravada, duplicada ou DLQ por outro motivo), na ordem de chegada. Se nenhuma mensagem do lote tiver offset válido, `OHIP_OFFSET` não é alterado.
   4. **Barreira (fencing), como último comando**: `UPDATE ohip_lease SET last_write_at = <agora do banco> WHERE lease_name = 'consumer:<chain>' AND epoch = :epoch`. Zero linhas → `ROLLBACK`, log crítico, encerra. O lock na linha do lease dura só até o commit (milissegundos), então a renovação não fica bloqueada por uma escrita lenta (ADR-0008).
   5. `COMMIT`. Só agora o offset está confirmado. Depois do commit: `ohip:seen:<uniqueEventId>` no Redis (best-effort).
5. **Falha do lote inteiro** (ex.: erro de banco que não é por linha): rollback; tenta de novo com backoff até `BATCH_MAX_RETRIES` (padrão 5). Se persistir com o Oracle respondendo, **bisseção em ordem**: divide o lote até isolar a mensagem culpada, commitando primeiro o prefixo (o offset avança só pelo prefixo commitado), depois a culpada vai para a DLQ `CONSUME` e o sufixo segue (ADR-0009). Se o Oracle estiver fora: conexão envenenada (item 2), estado `WAITING` sem assinar até o Oracle voltar, alerta. Nada é perdido porque o offset não avançou.
6. **Mensagem grande demais** (`WS_MAX_MESSAGE_BYTES`, padrão 16 MiB): o socket fecha; reconecta. Se a mesma condição se repetir `OVERSIZE_MAX_REPEATS` vezes (padrão 3) no mesmo offset → `STOPPED` + alerta crítico (evita laço infinito).

### 4.2 Publicação (publisher) — ADR-0002

Para cada chain com trabalho, o publisher pega a **cabeça da fila** daquela chain:

```sql
SELECT id, exchange_name, routing_key, message, attempts, next_attempt_at
  FROM (SELECT o.* FROM ohip_outbox o
         WHERE o.chain_code = :chain AND o.status = 'PENDING'
         ORDER BY o.id)
 WHERE ROWNUM <= :batch
```

- Se a primeira linha tiver `next_attempt_at` no futuro, **a chain inteira espera** (não pula a cabeça).
- Publica **uma mensagem por vez por chain, esperando o confirm** antes da próxima (chains diferentes em paralelo). Assim um `nack` nunca deixa a mensagem seguinte já aceita (sem reordenação silenciosa). Marca `SENT` após o `ack`. Janela de confirms em pipeline só com medição e ADR (Otimizador).
- Antes de publicar, confere o tamanho da mensagem contra o limite do broker (`BROKER_MAX_MESSAGE_BYTES`); acima → `FAILED` + DLQ direto.
- **Falha de conexão/canal com o broker** (broker fora, rede): não conta tentativa; backoff global do publisher; nada vai para a DLQ, por mais que dure a queda. Exceção: se o canal cair `CHANNEL_FAIL_ATTRIBUTION` vezes seguidas (padrão 3) **publicando a mesma linha** com o broker acessível (ex.: PRECONDITION_FAILED), a falha passa a contar como tentativa dessa linha, para uma mensagem venenosa não travar todas as chains.
- Toda marcação de linhas (`SENT`, tentativas, `FAILED`) termina com a mesma barreira de epoch do consumer, no lease `publisher`.
- **Falha da mensagem** (`nack` do broker): conta tentativa na linha, com backoff (`next_attempt_at`). Após `PUBLISH_MAX_ATTEMPTS` → `FAILED` + DLQ `PUBLISH`, alerta, e a chain continua (única quebra de ordem permitida).
- Mensagens sem fila ligada vão para o *alternate exchange* `ohip.events.unrouted` → fila `ohip.unrouted` (com limite de tamanho) + métrica. Nada some em silêncio.

### 4.3 Enriquecimento e normalização (enricher)

1. Consome a fila `ohip.enricher` (bindings configuráveis) e deduplica por `message_id`, **exceto** mensagens do exchange `ohip.reprocess` (§4.4), que sempre são processadas. O id só é marcado como visto depois do processamento com sucesso.
2. Lê o **payload bruto do Oracle** por `raw_event_id` (a mensagem da fila tem `detail` mascarado e não serve para normalizar).
3. Aplica a regra do `eventName` (RF-08). Sem regra → `processing_status = UNMAPPED` + métrica.
4. Se a regra pedir, chama a REST do OHIP com `primaryKey` + `hotelId`, usando cache e respeitando HTTP 429 (RF-09).
5. Grava com **`MERGE` condicional**: cada tabela de domínio guarda `source_offset_num NUMBER(20)` (o offset do evento, que é uma string só de dígitos, convertido para número). O `UPDATE` só acontece se o evento recebido **não for mais antigo** (`WHEN MATCHED THEN UPDATE ... WHERE t.source_offset_num <= :offset_num`). Assim, N enrichers em paralelo não sobrescrevem estado novo com antigo, e reprocessar o mesmo evento reaplica a regra (idempotente). Usa o offset, e não o `id` do bruto, porque um evento antigo regravado por retry de DLQ ganha `id` novo (ADR-0006). Atualização ignorada pela condição gera a métrica `ohip_merge_skipped_total`; muitas seguidas (ex.: OHIP reiniciou os offsets, D-6) disparam alerta e seguem o procedimento do RUNBOOK.
6. Atualiza `processing_status` (`NORMALIZED`/`ENRICHED`/`FAILED`); falha após N tentativas → DLQ `NORMALIZE`/`ENRICH`.

### 4.4 Reprocessamento

- `POST /events/{id}/reprocess` e o retry de DLQ `NORMALIZE`/`ENRICH` **inserem uma linha nova na outbox** destinada ao exchange **`ohip.reprocess`** (direct, ligado só à fila do enricher; coluna `exchange_name` na outbox). Bindings `ohip.#` de terceiros no `ohip.events` não recebem reprocessamentos. Nada é publicado fora da outbox.
- O retry de DLQ `PUBLISH` cria uma **linha nova** (copiando a mensagem) no fim da fila; a linha antiga fica `FAILED`, para não virar cabeça e travar a chain.
- O retry de DLQ `CONSUME` **não é feito pela API** (seria um segundo escritor da chain): a API marca `retry_requested_at` no item; o consumer da chain o processa na sua própria transação com barreira de epoch, sem mexer no offset.

### 4.5 Ordem e idempotência (resumo)

- Ordem até o Oracle: um consumer por chain, um executor de 1 thread, lotes em ordem de chegada.
- Ordem na fila: cabeça por chain, sem pular (§4.2).
- Idempotência: `UNIQUE(unique_event_id)` e `UNIQUE(chain_code, offset_value, primary_key)` no Oracle; Redis só depois do commit; consumidores da fila deduplicam por `message_id`; enricher com `MERGE` condicional por offset.
- Offset: string `^[0-9]{1,20}$` em `VARCHAR2(20)` (ADR-0006). Comparação numérica só em métricas, na validação de replay e na condição do `MERGE`.

### 4.6 Replay

1. A API grava o pedido `PENDING` (validações em API.md).
2. O consumer verifica pedidos a cada 5 s pela conexão de controle. Ao achar um: `complete` → drena e **commita normalmente** o que chegar → espera o servidor fechar.
3. Só depois do fechamento **e** com a fila interna vazia **e** sem lote em andamento, a transação roda **no próprio thread de escrita** (nenhum lote da drenagem pode commitar depois dela), com barreira de epoch: `UPDATE ohip_replay_request SET status = 'APPLIED' WHERE id = :id AND status = 'PENDING'` (rowcount 0 = cancelado nesse meio-tempo → segue sem replay) e `OHIP_OFFSET.last_offset = from_offset`. Commit.
4. Espera ≥ 10 s e assina com `from_offset`.

`from_offset` é o **último offset já processado antes da lacuna** (mesma semântica do guia: "last successfully processed event offset"). Assim o replay funciona se o OHIP tratar o offset como inclusivo (o evento volta e a deduplicação descarta) ou exclusivo (D-1). API e painel explicam isso ao operador.

Como a troca do offset acontece com a conexão fechada, nenhum lote pode sobrescrevê-la. Um crash antes do passo 3 deixa o pedido `PENDING`, e ele é aplicado antes do próximo `subscribe`. Um crash depois do passo 3 reinicia já a partir de `from_offset`.

## 5. Modelo de dados (Oracle 11g)

DDL em [`sql/`](../sql/) (rascunho, **não executar**). Identificadores ≤ 30 caracteres; IDs por `SEQUENCE` explícita no `INSERT`.

| Tabela | Finalidade | Destaques |
| --- | --- | --- |
| `OHIP_EVENT_RAW` | Evento bruto, fonte da verdade | `UNIQUE(unique_event_id)`, `UNIQUE(chain_code, offset_value, primary_key)`; `payload CLOB`; `processing_status` |
| `OHIP_OFFSET` | Último offset confirmado | PK `chain_code` |
| `OHIP_LEASE` | Liderança com prazo e epoch | PK `lease_name` (`consumer:<chain>`, `publisher`); `epoch`, `owner`, `expires_at`, `last_write_at` (hora do banco); linhas semeadas no provisionamento |
| `OHIP_OUTBOX` | Mensagens a publicar | `status IN (PENDING, SENT, FAILED)`; índice `(chain_code, status, id)` |
| `OHIP_DLQ` | Falhas por estágio | `stage IN (CONSUME, PUBLISH, NORMALIZE, ENRICH)`; `raw_message CLOB` |
| `OHIP_CONSUMER_STATUS` | Estado da conexão | último ping/pong/mensagem, `last_disconnect_at`, fechamentos consecutivos, próxima tentativa |
| `OHIP_REPLAY_REQUEST` | Pedidos de replay auditáveis | no máximo 1 `PENDING` por chain (índice único por função) |
| Tabelas de domínio | Normalizadas | **Só após a Q-1**; sempre com `source_event_raw_id` e `source_offset_num`. Chave natural `(chain_code, NVL(hotel_id, '#CHAIN'), primary_key)`, porque perfis podem vir com `hotel_id` nulo e um `NULL` não casa no `ON` do `MERGE` |

### 5.1 Retenção e expurgo

Job diário, em lotes (`DELETE ... WHERE ROWNUM <= :n`, commit por lote), nesta ordem:

1. `OHIP_DLQ` resolvida há mais de 90 dias.
2. `OHIP_OUTBOX` `SENT` há mais de 90 dias.
3. `OHIP_EVENT_RAW` com mais de 90 dias **e** `NOT EXISTS` em outbox e DLQ (linhas `FAILED` e DLQ abertas seguram o bruto até serem resolvidas).

Sem particionamento (licença adicional no 11g). Prazos finais dependem da Q-9.

### 5.2 Volume e desempenho

- Inserts em lote com `executemany` e `setinputsizes` para CLOB.
- IDs do lote reservados de uma vez: `SELECT ohip_event_raw_seq.NEXTVAL FROM dual CONNECT BY LEVEL <= :n`.
- Consumer: 1 conexão de escrita (thread única) + 1 conexão de controle (lease, status, replay). API: pool de sessões.

## 6. Contrato da mensagem na fila (v1)

Exchange `ohip.events` (topic, durável, com alternate exchange `ohip.events.unrouted`). Reprocessamentos usam o exchange separado `ohip.reprocess` (§4.4). Routing key `ohip.<modulo>.<EVENTO>`:

- `<modulo>`: mapa configurável `moduleName → código curto`, **sem diferenciar maiúsculas** (ex.: `reservation→rsv`, `profile→crm`); sem mapeamento, `moduleName` em minúsculas com espaços → `_`.
- `<EVENTO>`: `eventName` em maiúsculas com espaços → `_` (ex.: `UPDATE RESERVATION` → `UPDATE_RESERVATION`).

Propriedades AMQP: `message_id=<uniqueEventId>`, `content_type=application/json`, `delivery_mode=persistent`, header `x-schema-version: 1`.

```json
{
  "schema_version": 1,
  "unique_event_id": "5c00f504-3844-4a23-91af-feb150b06568",
  "offset": "97863",
  "chain_code": "CHAIN1",
  "hotel_id": "HOTEL1",
  "module_name": "RESERVATION",
  "event_name": "UPDATE RESERVATION",
  "primary_key": "1234567",
  "event_ts": "2026-09-30T16:45:48.000",
  "received_at": "2026-09-30T16:45:48.812Z",
  "publisher_id": "15951",
  "action_instance_id": "222222",
  "raw_event_id": 1001,
  "detail": [
    {"element_name": "FIRST NAME", "old_value": "", "new_value": "***", "scope_from": null, "scope_to": null}
  ]
}
```

- `detail` passa pela máscara LGPD (lista configurável de `elementName`, Q-9). Quem precisa do valor real lê o Oracle com permissão.
- Mudança incompatível → `schema_version` novo + ADR (RNF-15). Campo novo opcional não muda a versão.

## 7. Liderança, cache e Redis

**Liderança (ADR-0008)**: `OHIP_LEASE` no Oracle, com prazo (`LEASE_TTL`, padrão 30 s, renovado a cada 10 s pela conexão de controle de cada processo com lease) e `epoch` incrementado a cada aquisição. Toda transação de escrita termina com a barreira de epoch. Vale para `consumer:<chain>` e `publisher`. Não há trava no Redis (DV-12).

| Uso do Redis | Chave | Validade | Observação |
| --- | --- | --- | --- |
| Token OAuth | `ohip:token:<ambiente>:<chain>` | `exp − margem` | Por chain (DV-2). Sem Redis: cada processo busca o seu token |
| Dedup rápido | `ohip:seen:<uniqueEventId>` | 24 h | gravado **após** o commit |
| Recurso REST | `ohip:rest:<modulo>:<hotel>:<primaryKey>` | 30–60 s | coalesce rajadas |
| LOV | `ohip:lov:<hotel>:<tipo>` | 24 h | |
| Status da API | `ohip:api:status` | 5 s | protege o Oracle do polling do painel |
| Métricas dos processos | `ohip:metrics:<processo>:<instancia>` | 60 s | snapshot dos contadores a cada 15 s (§10) |

Redis fora: ingestão e publicação continuam; perdem-se só os atalhos e as métricas de contadores (alerta).

## 8. Escalabilidade

| Eixo | Estratégia | Limite |
| --- | --- | --- |
| Chains | uma instância `ohip-consumer@<chain>` por chain | app de cliente vale para 1 chain; máx. 100 apps por conta |
| Volume por chain | micro-lotes, fila interna limitada, `delta`/filtros, campos mínimos | 1 consumer por stream (limite OHIP) |
| Publicação | cabeça por chain, lotes | broker |
| Enriquecimento | N enrichers com `MERGE` condicional | cota REST do OHIP (HTTP 429 → backoff) |
| API/painel | sem estado, vários workers | Oracle (cache de 5 s) |
| Disponibilidade | VM única + `Restart=always`; depois VM passiva disputando o lease (padrão ativo/passivo da Oracle) | Q-6 |

## 9. Arquitetura do código

```
entrypoints ──▶ application ──▶ domain
     │               ▲
     └──▶ adapters ──┘ (implementam os ports)
```

- `domain/`: `Event`, `EventDetail`, `Offset`, `ChainCode`, limites de campo, routing key, máscara LGPD, política de fechamento/backoff, regras de replay (puras).
- `application/ports.py`: `EventStream`, `EventStore`, `OutboxRepository`, `MessagePublisher`, `TokenProvider`, `Lease`, `Cache`, `ResourceFetcher`, `MetricsSink`, `Clock`.
- `application/use_cases/`: `ProcessEventBatch`, `PublishOutbox`, `EnrichEvent`, `RequestReplay`, `ApplyReplay`, `ReprocessEvent`, `RetryDlqItem`.
- `adapters/`: `ohip_ws`, `ohip_rest`, `oracle`, `redis`, `rabbitmq`.
- `entrypoints/`: `consumer.py`, `publisher.py`, `enricher.py`, `api/`, `admin/`.

`import-linter` (ADR-0005): `domain`/`application` sem `oracledb`, `websockets`, `fastapi`, `flask`, `aio_pika`, `redis`, `httpx`; `entrypoints.admin` sem `adapters.oracle`, `adapters.redis`, `oracledb`, `redis`.

## 10. Observabilidade

- Logs JSON (`structlog`) com `chain_code`, `hotel_id`, `offset`, `unique_event_id`, `subscription_id`, `code_version`. Nunca token, secret, app key ou `detail` sem máscara.
- **Métricas**: cada processo mantém contadores em memória e grava um snapshot a cada 15 s em `ohip:metrics:<processo>:<instancia>` (Redis, TTL 60 s). O consumer também grava o essencial em `OHIP_CONSUMER_STATUS` (estado, ping/pong, RTT, reconexões, fechamentos). O `/metrics` da API junta Oracle + Redis no formato Prometheus (formato texto gerado à mão, sem dependência nova; `prometheus-client` só com ADR).
- Métricas: estado da conexão, último offset, eventos/min, duplicados, ignorados, não mapeados, DLQ por estágio, atraso (`received_at − event_ts`, `persisted_at − received_at`), pendentes e idade da mais antiga na outbox, mensagens sem rota, reconexões e códigos de fechamento, RTT do heartbeat, tamanho da fila interna.
- Alertas (RF-11): sem eventos > X min em horário comercial; conexão caída > Y min (bem antes da retenção de 7 dias); DLQ > Z; outbox mais antiga > W min; 4403/4406/`STOPPED` imediatos; 4409 repetido; Redis fora; lease perdido.

## 11. Segurança e LGPD

- Segredos (clientId, clientSecret, appKey, senha do banco, tokens de serviço da API) só por ambiente/cofre; `.env.example` sem valores. A app key vai em claro no `connection_init` (DV-7) e é tratada como segredo.
- Token OAuth no Redis: Redis com senha (ACL) e acesso só pela rede interna (Q-12).
- `detail` contém dados pessoais: máscara nos logs, na API (inclusive no export) e na fila; bruto íntegro no Oracle com acesso restrito; BI lê por view mascarada (Q-9).
- Dados de cartão: nunca assinar eventos de pagamento; `elementName` de cartão é mascarado e gera alerta.
