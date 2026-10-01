# ADR-0007 — Ciclo de vida da conexão, heartbeat e tratamento de fechamentos

- Status: Aceito (aprovado em 2026-10-01 com a Fase 0)
- Data: 2026-10-01

## Contexto

O guia Oracle detalha regras que o PRD resume: `pong` pode ser adiado durante rajadas (backpressure), o cliente não deve fechar o socket depois do `complete`, o lockout do 4409 é de cerca de 2 minutos, e existem os códigos 4406 e 4408 além dos listados no PRD.

## Decisão

**Conexão**
- URL `wss://<gateway>/subscriptions?key=<sha256 hex minúsculo da app key>`; subprotocolo `graphql-transport-ws`. O `websockets` não deve enviar seus próprios pings de controle de forma que conflite com o protocolo: `ping_interval=None` e heartbeat no nível do protocolo GraphQL (`{"type":"ping"}`).
- `connection_init` em até 5 s com `{"Authorization": "Bearer <token>", "x-app-key": "<appKey>"}`; espera `connection_ack` com timeout configurável (padrão 10 s).
- `subscribe` com `id` GUID novo, gerado após o `connection_init`; o mesmo `id` no `complete`.
- Opcional (`STATUS_CHECK_ENABLED`, **padrão desligado** até validar o D-4 no sandbox; intervalo mínimo de 30 s entre consultas por causa do limite de 12 req/min, D-5): `query { connection { id status } }` antes do `subscribe`; se não for `Inactive`, espera com jitter. TODO(confirmar-doc): formato exato da operação no graphql-transport-ws, a validar no sandbox (D-4).

**Heartbeat**
- Envia `ping` a cada 15 s; responde `pong` a todo `ping` do servidor.
- Prova de vida = `pong` **ou** qualquer `next`. Reconecta se não houver prova de vida em `max(180 s, 4 × SRTT + jitter)` (política recomendada pela Oracle).

**Renovação de token**
- Quando faltar `TOKEN_REFRESH_MARGIN` (padrão 5 min) para o `exp`: `complete` → processa o que chegar → espera o fechamento do servidor (timeout `DRAIN_TIMEOUT`, padrão 30 s; só então fecha do lado do cliente) → espera até completar 10 s desde o `complete` → token novo → reconecta com o último offset.
- O token é buscado antes da espera terminar, para não somar latência.

**Fechamentos**

| Código | Ação |
| --- | --- |
| 1000 | Normal (após `complete`). Reconecta se não estiver parando. |
| 4401 | Invalida o token em cache, busca outro, reconecta. 3 vezes seguidas → alerta (provável hash da app key ou credencial errada). |
| 4403 | `STOPPED` + alerta crítico. Exige ação humana no Developer Portal. |
| 4406 | `STOPPED` + alerta: bug no handshake (subprotocolo). |
| 4408 | Reconecta com backoff; repetido → alerta (init lento). |
| 4409 | Espera 120 s + jitter (0–30 s); alerta se persistir por mais de `LOCK_ALERT_AFTER` (padrão 10 min). |
| 4504 | Espera 15 s e reconecta preservando o offset. |
| Rede/timeout/outros | Backoff exponencial com jitter, **começando em 10 s** e com teto de 60 s (fica abaixo do limite de 12 req/min, D-5). |
| Mensagem acima de `WS_MAX_MESSAGE_BYTES` (padrão 16 MiB) | Reconecta. Se repetir `OVERSIZE_MAX_REPEATS` vezes (padrão 3) no mesmo offset → `STOPPED` + alerta crítico (evita laço infinito). |

- **Regra dos 10 s, inclusive após crash ou restart** (guia, Scaling Recommendations: "If the last disconnect time is unknown (for example, after a crash or restart), always wait at least 10 seconds"):
  - todo `complete`/fechamento grava `last_disconnect_at` em `OHIP_CONSUMER_STATUS` **com o relógio do banco**, e a espera restante é calculada em SQL (VMs diferentes não comparam relógios);
  - ao iniciar (ou ao assumir o lease): se o último estado gravado for `WAITING` ou `STOPPED`, espera até completar 10 s desde `last_disconnect_at`; se for qualquer outro (o processo anterior caiu sem registrar a desconexão), espera **10 s fixos** a partir de agora; `last_disconnect_at` nulo (primeira partida) também leva a 10 s fixos;
  - systemd com `RestartSec=10` como proteção extra.
- Fila interna cheia por mais de `INTAKE_STALL_TIMEOUT` ou Oracle fora: a conexão é marcada como **envenenada**. O `put` pendente é cancelado, o não commitado é descartado, **nada mais desta conexão é gravado** (nem o que chegar durante a drenagem), envia `complete`, espera o fechamento e reconecta pelo último offset commitado (ARCHITECTURE §4.1). Fora desse caso, a drenagem grava normalmente o que chegar.
- Replay pedido: `complete` → drena → servidor fecha → troca do offset com a conexão fechada → ≥ 10 s → `subscribe` (ARCHITECTURE §4.6).
- Todo `subscribe` envia o último offset persistido, se existir (independente das 24 h), e loga `subscription_id`, chain, offset e horário (dados pedidos pelo suporte Oracle).
- SIGTERM: `complete` → drena → commit do lote pendente → grava `last_disconnect_at` → libera o lease (só se ainda for o dono, com o mesmo epoch — ADR-0008) → sai.

## Consequências

- Mais estados do que o PRD descreve, todos cobertos por testes contra o servidor simulado (Fase 5).
- O servidor simulado precisa reproduzir: rajadas com `pong` atrasado, fechamento só pelo servidor, 4401/4403/4406/4408/4409/4504, mensagem grande demais, reconexão em menos de 10 s (deve gerar 4409).
