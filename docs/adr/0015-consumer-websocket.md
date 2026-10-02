# ADR-0015 — Consumer WebSocket (Fase 5)

- Status: Proposto (Fase 5)
- Data: 2026-10-02

## Contexto

A Fase 5 junta as peças das Fases 2 a 4 no processo `ohip-consumer`: protocolo graphql-transport-ws (ADR-0007), gravação sem perda (ADR-0002/0009), lease (ADR-0008) e token (ADR-0014). O formato das mensagens foi conferido no guia oficial ("Subscribing and Consuming Events", "Code Examples of Subscribe Messages") e no schema copiado em `vendor/` (ADR-0012).

## Decisão

1. **Mensagens** (`domain/protocol.py`, puro):
   - formato do guia: `input` escrito na query (`newEvent(input: { chainCode: "X" offset: "N" hotelCode: "A,B" delta: true })`) e `variables.input` só com o `chainCode`, `extensions: {}`, `operationName: null`;
   - a query pede todos os campos que o parser lê (teste de contrato contra o schema); `dataValueMapping` não é pedido (D-12);
   - os valores escritos na query passam pelos padrões do schema antes (sem aspas possíveis);
   - status da conexão (D-4) num `subscribe` com `id` próprio, como no cliente oficial.
2. **Sessão** (`application/use_cases/consume_chain.py`): `connection_init` logo após abrir; `connection_ack` com prazo; status opcional; `subscribe` com GUID e o último offset **confirmado no banco** (nunca um offset em memória). Quatro tarefas:
   - **leitura**: `next` da assinatura → fila interna; `ping` → `pong`; `error`/`complete` do servidor → drenagem e reconexão (D-10); frames desconhecidos → métrica e log;
   - **gravação**: micro-lotes → `ProcessEventBatch`;
   - **heartbeat**: `ping` a cada 15 s; prova de vida = `pong` ou `next`; prazo `max(180 s, 4 × SRTT + jitter)`;
   - **controle**: parada, lease perdido, token na margem, replay pendente e retry de DLQ CONSUME.
3. **Encerramento**: `complete` → continua lendo e gravando → espera o servidor fechar (`OHIP_DRAIN_TIMEOUT_S`). Só depois do prazo, sem prova de vida ou com falha antes de assinar o cliente fecha o socket.
4. **Conexão envenenada**:
   - gatilhos: `put` na fila travado além de `CONSUMER_INTAKE_STALL_TIMEOUT_S`, banco indisponível ou lease perdido;
   - efeito: a fila é descartada, nada mais da conexão é gravado, `complete`, e a próxima assinatura parte do offset confirmado;
   - testado com reentrega pelo servidor simulado, sem lacuna nem duplicado.
5. **Laço externo**:
   - lease;
   - regra dos 10 s pelo relógio do banco (desconhecido → espera integral, também quando o status está indisponível);
   - replay pendente aplicado com a conexão fechada;
   - decisão de fechamento (`domain/connection.py`) e espera interrompível;
   - sessão saudável (recebeu evento ou ficou assinada por `healthy_after_s`) zera as falhas seguidas;
   - drenagem por token ou replay reconecta só com o intervalo mínimo;
   - 4401 invalida o token;
   - recusa do OAuth ou HTTP 400 no upgrade → alerta crítico e backoff (configuração);
   - `STOP` (4403, 4406, mensagem grande demais N vezes no mesmo offset) → `STOPPED` e espera a parada, sem reconectar.
5a. **Nenhuma tarefa morre em silêncio**:
   - erro inesperado na gravação → log, envenenamento e drenagem (sem esperar a fila encher);
   - erro na leitura ou no heartbeat → log e fim da sessão (`INTERNAL_ERROR`, backoff);
   - leitura ou gravação que termina antes de a sessão acabar, sem envenenamento nem drenagem → falha; exceções de tarefas auxiliares são registradas;
   - erro não tratado no processo vai para o log estruturado, com a chain, e o processo sai com código 1.
5b. **Parada durante `ACQUIRING`**: a espera pelo lease é interrompível (instância passiva sai no SIGTERM, sem SIGKILL). **Sem lease, o processo não grava nada em `OHIP_CONSUMER_STATUS`**: a linha é da chain, e uma passiva sobrescreveria o estado da ativa, anulando a regra dos 10 s após crash.
5c. **OAuth recusado** N vezes seguidas (`auth_rejected_max`, padrão 5) → `STOPPED`: credencial errada exige ação humana e evita bloqueio da conta por excesso de logins recusados.
6. **Token**: o consumer não calcula margem; pergunta ao `TokenProvider.refresh_due(token)` (mesma regra do `get`, inclusive a meia-vida de tokens curtos). Duas margens diferentes provocavam um laço de reconexão com o mesmo token (bug achado nos testes).
7. **Adapter** (`adapters/ohip_ws/client.py`, `websockets` 17):
   - `ping_interval=None` (o heartbeat é o do protocolo), `max_size` = `OHIP_WS_MAX_MESSAGE_BYTES` (1009 → `MessageTooLargeError`), sem compressão;
   - subprotocolo confirmado pelo servidor, senão `HandshakeRejectedError`;
   - HTTP 4xx no upgrade → `HandshakeRejectedError`; rede → `ConnectionClosedError(None)`;
   - URL `wss://<host>/subscriptions?key=<sha256>`, fora de `repr` e de log.
8. **Processo** (`entrypoints/consumer.py`, script `ohip-consumer`):
   - compõe Oracle (escrita dedicada de 1 thread + controle), Redis, OAuth e WebSocket;
   - SIGTERM/SIGINT → parada ordenada;
   - snapshot de métricas no Redis a cada 15 s;
   - `instance_id` = host:pid:aleatório.
9. **Testes**:
   - `tests/fakes/fake_ohip_server.py`: servidor real em localhost que reproduz 4401/4403/4406/4408/4409/4504, HTTP 400 por chave, lockout em reconexão rápida, fechamento só pelo servidor, `pong` ausente, `ping` do servidor, frame `error`, mensagem gigante e status query;
   - 28 cenários ponta a ponta com tempos em milissegundos, estáveis em execuções repetidas; incluem o offset **inclusivo** (D-1, reenvio deduplicado), eventos que chegam **depois** do `complete` (gravados na drenagem normal, descartados na conexão envenenada, sem salto de offset) e falhas inesperadas de gravação e leitura;
   - unitários de protocolo, fila interna e adapter;
   - teste de contrato da seleção de campos.

## Consequências

- O consumer está completo contra o simulador. A validação no sandbox OHIP (D-1, D-4, D-9, D-10, D-11) e no Oracle real (Q-17) continua pendente e não depende de mudança de código, só de ambiente.
- Unit do systemd, Nginx e deploy ficam para a Fase 10 (`RestartSec=10`).
