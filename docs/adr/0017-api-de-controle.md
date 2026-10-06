# ADR-0017 — API de controle FastAPI (Fase 7)

- Status: Proposto (Fase 7)
- Data: 2026-10-05

## Contexto

O contrato da API foi aprovado na Fase 0 (docs/API.md). Os casos de uso de escrita (`RequestReplay`, `CancelReplay`, `ReprocessEvent`, `RetryDlqItem`) e os stores Oracle deles existem desde as Fases 2 e 3 e são assíncronos: os stores rodam o driver síncrono por `Session.run`. O ADR-0003 exige rotas `def` (threadpool do FastAPI) para tudo que toca o Oracle, e o CLAUDE.md proíbe `async def` nessas rotas. Faltavam as consultas (status, eventos, outbox, DLQ, replays), a autenticação, o formato de erro, `/health`, `/ready`, `/metrics` e o processo.

## Decisão

1. **Rotas `def` e ponte para os casos de uso** (`entrypoints/api/runtime.py`):
   - cada rota roda num thread do threadpool e executa o caso de uso com `asyncio.run` **nesse thread**;
   - os stores da API usam `InlineSession` (`adapters/oracle/database.py`): o `run` chama `PooledSession.call` no próprio thread, sem executor;
   - o Oracle nunca é chamado do event loop do Uvicorn, e os casos de uso já testados são reaproveitados sem cópia síncrona;
   - nada preso a um event loop é compartilhado entre requisições: o Redis da API usa o cliente síncrono do mesmo pacote `redis`.
2. **Consultas** pelo port `MonitoringStore` (`application/ports/monitoring.py`) e o caso de uso `Monitoring` (`application/use_cases/monitoring.py`), que aplica a máscara e monta as páginas:
   - paginação por chave: `id` decrescente, `?cursor=` é o último `id` visto, `?limit=` 1–200 (padrão 50). O store lê `limit + 1` linhas com `ROWNUM` para saber se há próxima página (`next_cursor`);
   - filtros `from`/`to` de `/events` valem para `received_at` (índice `ohip_event_raw_ix_recv`); `event_name` é normalizado como na gravação; `module_name` compara sem diferenciar maiúsculas;
   - status por chain = linhas de `OHIP_CONSUMER_STATUS` + `OHIP_OFFSET` + agregados de outbox (`PENDING`/`FAILED`), DLQ aberta (`resolved_at IS NULL`) e eventos dos últimos 5 min (eventos/min = total ÷ 5; p95 de `received_at − event_ts` com `PERCENTILE_CONT`). O relógio é o do banco;
   - a lista de DLQ não devolve `raw_message` nem `stack_trace` (podem ter dado pessoal); quem precisa lê o Oracle com permissão.
3. **Autenticação e perfis**:
   - `Authorization: Bearer <token>`; o SHA-256 do token é procurado em `API_SERVICE_TOKENS` (hash → `read` | `admin`). Sem token ou token desconhecido → 401; perfil `read` em rota `admin` → 403;
   - rotas `admin` exigem `X-Actor` (1 a 100 bytes UTF-8, sem caractere de controle; vai para `requested_by`/`resolved_by`/`cancelled_by`). Sem `X-Actor` → 403; inválido → 400;
   - `/health`, `/ready` e `/metrics` ficam sem token: só a rede interna chega neles (o Nginx bloqueia de fora, Fase 10).
4. **Erros** no formato `{"error": {"code", "message", "request_id"}}`:

   | Origem | HTTP | `code` |
   | --- | --- | --- |
   | validação do FastAPI | 400 | `VALIDATION_ERROR` (sem ecoar o valor recebido) |
   | regras de replay do domínio | 400 | o `code` do erro (ex.: `REPLAY_OFFSET_INVALID`) |
   | outro erro de domínio | 400 | `VALIDATION_ERROR` (só os códigos de replay estão no contrato) |

   A mensagem dos erros de domínio é fixa por tipo e nunca repete o valor recebido (nem o último offset confirmado).
   | `ReplayForwardNotAllowedError` | 422 | `REPLAY_FORWARD_NOT_ALLOWED` |
   | `NotFoundError` e subclasses | 404 | `NOT_FOUND` |
   | `ReplayAlreadyPendingError` / `ReplayNotPendingError` | 409 | o `code` do erro |
   | `InvalidOperationError` | 409 | `INVALID_STATE` |
   | `StoreUnavailableError` | 503 | `DEPENDENCY_UNAVAILABLE` |
   | `StoreOperationError` e erro inesperado | 500 | `INTERNAL_ERROR` (detalhe só no log) |

   O `request_id` vem do header `X-Request-ID` (se for `[A-Za-z0-9._-]{1,64}`) ou é gerado; volta no header da resposta e entra em todo log da requisição.
5. **Máscara LGPD**: `detail` sai sempre mascarado (mesma `MaskingPolicy` da fila). `?unmasked=true` em `GET /events/{id}` e no export só com perfil `admin` e `X-Actor`, e grava o log de auditoria `auditoria_dado_sem_mascara`. O export devolve o corpo no contrato v1, o `newEvent` (com `detail` mascarado por `mask_payload`) e o contexto: status, outbox e DLQ ligados.
6. **Cache**:
   - `GET /api/v1/status`: JSON no Redis em `ohip:api:status` por `REDIS_API_STATUS_TTL_S` (5 s), para o painel com vários workers não martelar o Oracle, e uma cópia local no worker com o mesmo prazo (cobre o Redis fora). Um único cálculo por worker de cada vez (*single-flight*): quem chega durante o cálculo espera e reaproveita o resultado;
   - `/metrics` e `/ready`: cache em memória de cada worker (`API_METRICS_CACHE_S`, `API_READY_CACHE_S`, 5 s).
7. **`/metrics`** em texto Prometheus gerado à mão (sem dependência nova): gauges por chain tirados do status, mais os contadores dos snapshots `ohip:metrics:<processo>:<instancia>` do Redis com os rótulos `process` e `instance`. Nome ou rótulo fora do padrão Prometheus é descartado; valores de rótulo são escapados.
   - Os processos registram o nome da sua chave no conjunto `ohip:metrics_index` (mesmo pipeline do `SET` com TTL). A API lê o índice e faz `MGET`; chave vencida sai do índice na leitura. Nada de `SCAN`: o keyspace tem uma chave `ohip:seen:*` por evento (achado da revisão).
8. **`/ready`**: Oracle (`SELECT 1 FROM dual`), Redis (`PING`) e RabbitMQ (abre e fecha uma conexão AMQP, com o prazo `RABBITMQ_CONNECT_TIMEOUT_S`). 503 se algum falhar, com o resultado por dependência; o erro mostra só o tipo da exceção, nunca a URL.
9. **Processo `ohip-api`** (`entrypoints/api/server.py`):
   - Uvicorn com a fábrica `create_app`, `API_WORKERS` workers, `API_HOST` (padrão `127.0.0.1`, atrás do Nginx) e `API_PORT`;
   - `/docs`, `/redoc` e `/openapi.json` ficam desligados em `APP_ENVIRONMENT=producao` (o contrato executável é conferido nos testes);
   - o access log do Uvicorn fica desligado: ele grava a query string, que pode ter `primary_key`. A API grava uma linha própria por requisição com método, rota, status, duração, perfil e ator;
   - o lifespan abre e fecha o pool Oracle, o Redis e o executor. O consumer nunca roda aqui (ADR-0001).
10. **Configuração**:
    - `ApiSettings` ganha `host`, `port`, `workers`, `oracle_call_timeout_ms`, `metrics_cache_s` e `ready_cache_s`. `API_ORACLE_CALL_TIMEOUT_MS` (padrão 15 s) é o prazo de cada chamada ao Oracle feita pela API, separado do `ORACLE_CALL_TIMEOUT_MS` do consumer: uma busca cara não segura uma conexão do pool (4 por worker) por 60 s;
    - `OhipRoutingSettings` lê só `OHIP_MODULE_CODES` (routing key do reprocessamento), para a API não receber app key nem client secret;
    - a API carrega `App`, `Oracle`, `Redis`, `RabbitMQ`, `Api`, `OhipRouting` e `Log`.

## Consequências

- Uma requisição custa um `asyncio.run` (criar e fechar um event loop) além da consulta. Só otimizar com medição (Fase 10).
- O status mostra o que o consumer grava hoje: estado, instância, offset, último fechamento e `last_disconnect_at`. `subscription_id`, `last_message_at`, `reconnects`, `consecutive_failures`, `next_attempt_at`, `token_expires_at`, ping/pong e RTT saem `null`/0 até o consumer gravá-los (pendência registrada no PLAN).
- Rota nova ou mudança de contrato continua exigindo ADR (RNF-15); o teste `tests/contract/test_api_contract.py` confere as rotas do OpenAPI contra a tabela do API.md.
- O pool de cada worker tem `ORACLE_POOL_MAX` conexões; com todas ocupadas, a requisição espera `ORACLE_POOL_WAIT_TIMEOUT_MS` e devolve 503.
- Filtros pouco seletivos de `/events` (`processing_status` raro, `module_name`, janela larga) e `/outbox?status=FAILED` sem chain podem percorrer o PK de trás para frente. Os planos dependem das estatísticas: o checklist da Q-17 inclui `EXPLAIN PLAN` dessas listagens com volume realista; se preciso, exigir janela `from` ou criar índice (com ADR).
- `idade da mais antiga` em `/outbox` e `/status` usa o relógio do banco, o mesmo de `created_at`.
