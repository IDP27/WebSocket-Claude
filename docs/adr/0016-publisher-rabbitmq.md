# ADR-0016 — Publisher e RabbitMQ (Fase 6)

- Status: Proposto (Fase 6)
- Data: 2026-10-02

## Contexto

O caso de uso `PublishOutbox` (Fase 2) e o `OracleOutboxStore` (Fase 3) já cumprem o ADR-0002: cabeça por chain, uma mensagem por vez esperando o confirm e falha de broker sem contar tentativa. Faltavam o adapter do broker, o laço do processo e a topologia. A Q-13 (quem declara as filas) segue com o padrão até a resposta.

## Decisão

1. **Adapter** (`adapters/rabbitmq/publisher.py`, `aio-pika` 10): canal com publisher confirms, `mandatory=True` e prazo de confirm `RABBITMQ_CONFIRM_TIMEOUT_S` (padrão 30 s).
   - `Basic.Ack` → `ACKED`.
   - `Basic.Nack` ou mensagem devolvida → `NACKED`: conta tentativa.
   - `Channel.Close` do broker com código de erro de canal (311, 403, 404, 405, 406) e a conexão de pé → `ChannelClosedError`: conta tentativa só se repetir, ADR-0011.
   - Canal já fechado do nosso lado (`ChannelInvalidStateError`) ou fechado sem código de canal → `BrokerUnavailableError`. Numa queda de conexão o aiormq pode marcar os canais como fechados antes da conexão; sem essa regra, cabeças inocentes gastariam tentativa numa rede instável (achado da revisão).
   - Conexão perdida, broker fora ou **confirm sem resposta** → `BrokerUnavailableError`, sem contar tentativa. Sem confirm, o resultado é desconhecido: a mensagem pode sair de novo, e os consumidores deduplicam pelo `message_id`.
   - Propriedades AMQP do ARCHITECTURE §6: `message_id`, `content_type`, `delivery_mode=persistent` e `x-schema-version`.
   - A URL, que tem a senha, fica fora de `repr` e de mensagens de erro.
1a. **Um canal por chain** na mesma conexão (`OutgoingMessage.lane`). O broker fecha o canal inteiro quando recusa uma mensagem, e o aiormq rejeita todas as publicações pendentes daquele canal. Com um canal compartilhado por várias chains em paralelo, a queda causada pela mensagem de uma chain contaria como falha das cabeças das outras e levaria mensagens inocentes a `FAILED` (achado da revisão). Com um canal por chain, e uma publicação por vez por chain, a regra do ADR-0011 nº 7 vale. A conexão tem uma geração: um reset atrasado nunca derruba uma conexão mais nova.
2. **Topologia nossa (Q-13, padrão)**, declarada pelo publisher na conexão, de forma idempotente:
   - `ohip.events`: topic, durável, com `alternate-exchange` = `ohip.events.unrouted`;
   - `ohip.events.unrouted`: fanout, ligado à fila `ohip.unrouted` (`x-max-length` = `RABBITMQ_UNROUTED_MAX_LENGTH`, padrão 100.000, `x-overflow=drop-head`);
   - `ohip.reprocess` e a fila `ohip.enricher` ligada a ele. As bindings do enricher em `ohip.events` são da Fase 9; filas de terceiros, cada time declara.
   - Argumentos divergentes ou permissão negada na declaração (PRECONDITION_FAILED, ACCESS_REFUSED) → `BrokerMisconfiguredError`: alerta crítico e saída com código 2. Outros fechamentos de canal na declaração são indisponibilidade. A unit do systemd (Fase 10) precisa de `RestartPreventExitStatus=2`, senão o `Restart=always` reinicia a cada 10 s.
   - A fila `ohip.enricher` é declarada sem argumentos; o enricher (Fase 9) precisa declará-la igual ou passivamente.
3. **`ohip.reprocess` é fanout, não direct** (corrige o ARCHITECTURE §4.4). As mensagens de reprocessamento mantêm a routing key `ohip.<modulo>.<EVENTO>`. Num exchange direct, a binding da fila do enricher precisaria de uma chave igual a cada uma delas, e nada seria entregue. Com fanout, só a fila do enricher fica ligada e recebe tudo, qualquer que seja a chave. Terceiros continuam sem receber reprocessamentos.
4. **Laço** (`application/use_cases/publisher_service.py`):
   - lease `publisher` interrompível; sem lease, não grava nada;
   - a cada rodada, as chains com `PENDING`, até `PUBLISHER_MAX_PARALLEL_CHAINS` em paralelo; cada chain publica a cabeça da sua fila (até `PUBLISHER_BATCH_LIMIT` linhas);
   - chain com a cabeça em backoff é pulada até o horário dela, e as outras seguem;
   - broker fora → backoff global exponencial (`PUBLISHER_BROKER_BACKOFF_*`);
   - banco fora → espera e tenta de novo;
   - lease perdido → sai sem liberar;
   - ocioso → `PUBLISHER_POLL_INTERVAL_S`, ou menos se uma cabeça sai do backoff antes.
   - falha de banco numa chain durante a rodada → espera do banco (sem laço sem pausa); esperas vencidas e chains sem `PENDING` saem da lista de bloqueadas.
5. **Processo `ohip-publisher`** (`entrypoints/publisher.py`): Oracle por `PooledSession` com executor do tamanho do pool, broker, métricas no Redis e SIGTERM/SIGINT para parada ordenada. `instance_id` passa para `entrypoints/common.py`, para os processos não importarem uns aos outros.
6. **Testes**:
   - unitários do laço sobre o fake em memória: ordem por chain, cabeça em backoff sem travar as outras, backoff exponencial do broker, banco fora, lease perdido, topologia divergente, ocioso e parada sem lease;
   - adapter com `aio-pika` falso: propriedades, topologia, nack, devolução, canal × conexão, código de fechamento, canal fechado localmente, queda com duas chains, timeout, reconexão;
   - teste `@rabbitmq` num vhost descartável (`TEST_RABBITMQ_URL` + `TEST_RABBITMQ_DISPOSABLE=sim`): roteamento, alternate exchange, reprocesso em fanout e queda de conexão com duas chains publicando.

## Consequências

- Variáveis novas: `PUBLISHER_*` e `RABBITMQ_UNROUTED_QUEUE`, `RABBITMQ_UNROUTED_MAX_LENGTH`, `RABBITMQ_CONNECT_TIMEOUT_S`, `RABBITMQ_CONFIRM_TIMEOUT_S`.
- Uma janela de confirms em pipeline (mais vazão) só com medição e outro ADR (Otimizador).
