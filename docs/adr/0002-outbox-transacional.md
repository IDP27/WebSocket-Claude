# ADR-0002 — Outbox transacional e ordem de publicação por chain

- Status: Aceito (aprovado em 2026-10-01 com a Fase 0)
- Data: 2026-10-01

## Contexto

Gravar no Oracle e publicar no RabbitMQ não cabem numa transação. O PRD pede publicação "em ordem de offset" e, com o RabbitMQ fora, que os eventos fiquem `PENDING` e drenem em ordem (RF-10). Não define o que fazer quando uma mensagem específica falha repetidamente.

## Decisão

1. O consumer grava evento, offset e linha `PENDING` em `OHIP_OUTBOX` na mesma transação. **Nenhum outro caminho publica na fila**: reprocessamentos e retries também inserem linhas na outbox.
2. O `ohip-publisher` é instância única, garantida pelo lease `publisher` em `OHIP_LEASE` (ADR-0008), renovado por uma conexão de controle própria; toda transação que marca linhas termina com a barreira de epoch.
3. **Cabeça da fila por chain**: para cada chain, lê as linhas `PENDING` em ordem de `id`. Se a primeira tem `next_attempt_at` no futuro, **a chain inteira espera**; nunca pula a cabeça. Outras chains seguem.
4. **Dois tipos de falha:**
   - *Falha de conexão/canal com o broker* (broker fora, rede, canal fechado): não conta tentativa em nenhuma linha. O publisher entra em backoff global (10 s → 60 s) e tenta reconectar. Por mais longa que seja a queda, nada vai para a DLQ e a ordem se mantém. **Exceção:** se o canal cair `CHANNEL_FAIL_ATTRIBUTION` vezes seguidas (padrão 3) ao publicar a **mesma** linha com o broker acessível (ex.: PRECONDITION_FAILED por tamanho), conta como falha da mensagem; uma mensagem venenosa não trava todas as chains. Mensagens acima de `BROKER_MAX_MESSAGE_BYTES` vão para `FAILED` + DLQ antes de publicar.
   - *Falha da mensagem* (`nack` do broker para aquela publicação): conta tentativa na linha, com backoff (`next_attempt_at`). Após `PUBLISH_MAX_ATTEMPTS` (padrão 10), a linha vira `FAILED`, ganha item de DLQ `PUBLISH`, gera alerta e a chain continua. É a única quebra de ordem permitida.
5. Publicação com publisher confirms, **uma mensagem por vez por chain** esperando o confirm (chains em paralelo). Com confirms em pipeline, um `nack` da mensagem k chegaria depois de k+1 já aceita, reordenando sem passar por `FAILED`. Janela em pipeline só com medição (RNF-13) e ADR. `SENT` só após o `ack`.
6. Exchange com *alternate exchange* `ohip.events.unrouted` → fila `ohip.unrouted` (com limite de tamanho) e métrica. Uma mensagem sem binding não some com `ack` silencioso.
7. Reprocessamentos vão para o exchange separado `ohip.reprocess` (coluna `exchange_name` na outbox), ligado só ao enricher; terceiros com binding `ohip.#` não os recebem.
8. Retry de DLQ `PUBLISH` cria uma **linha nova** no fim da fila (copiando a mensagem); a antiga fica `FAILED`. Assim o retry não vira cabeça e não trava a chain.
9. Entrega na fila é "pelo menos uma vez": `message_id = uniqueEventId`; consumidores deduplicam.
10. A ordem é a de `OHIP_OUTBOX.id`, não a do offset (string — ADR-0006). Um único escritor por chain reserva os ids em ordem. Em RAC, a sequence precisa ser `ORDER` (Q-11).

## Consequências

- Queda longa do RabbitMQ não afeta a ingestão nem a ordem; a outbox acumula e drena (alerta pela idade da mais antiga).
- Uma mensagem rejeitada pelo broker atrasa a sua chain até esgotar as tentativas (padrão ~30 min de backoff acumulado).
- Vazão por chain limitada pelo tempo de ida e volta do confirm (local: ~1 ms, centenas de mensagens/s por chain); medido na Fase 10.
- A outbox `SENT` precisa de expurgo (RNF-11).

## Alternativas descartadas

- Publicar direto do consumer após o commit: perde mensagens numa queda entre o commit e o publish.
- Pular a mensagem que falhou: quebra a ordem por chain sem visibilidade.
- Contar tentativas também nas falhas de conexão: numa queda longa, uma mensagem por chain iria para a DLQ a cada ciclo (achado do Revisor na Fase 0).
