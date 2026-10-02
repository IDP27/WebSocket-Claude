# ADR-0014 — Token OAuth, lease e Redis (Fase 4)

- Status: Proposto (Fase 4)
- Data: 2026-10-02

## Contexto

A Fase 4 entrega o que o consumer (Fase 5) e o publisher (Fase 6) precisam antes de conectar: o token OAuth da OHIP, a liderança por lease no Oracle (ADR-0008) e os atalhos no Redis (ADR-0009, DV-2, DV-12). As regras de token vêm da spec oficial e do User Guide (OHIP_APIS.md §4). Q-10 (modo de autenticação) e Q-12 (segurança do Redis) seguem com os padrões até a resposta.

## Decisão

1. **Token** (`application/use_cases/token_provider.py`):
   - `TokenProvider` guarda o token em memória e no Redis (`ohip:token:<ambiente>:<chain>`, DV-2), compartilhado pelos processos, porque pedir token é cobrado para parceiros.
   - Renova `OHIP_TOKEN_REFRESH_MARGIN_S` (padrão 300 s) antes do `exp`; a Oracle pede ao menos 120 s.
   - Uma só emissão por vez por processo (`asyncio.Lock`).
   - Redis fora não impede a emissão.
   - `invalidate()` depois de um 4401.
   - Token com validade menor ou igual à margem: a margem efetiva cai para metade da vida do token, com `log.error` e a métrica `ohip_token_short_lived_total`, e o token vai para o cache com esse TTL (evita emitir a cada chamada). A vida original viaja com o token no Redis (`lifetime_s`), para todo processo calcular a mesma margem e reaproveitar o token.
   - `invalidate()` roda sob o mesmo lock do `get`: uma busca em curso não devolve o token recusado.
   - O prazo local do lease conta do **início** da chamada, tanto no `acquire` quanto no `renew`; vencido o prazo local, o `renew_once` não ressuscita o lease.
2. **Emissor OAuth** (`adapters/ohip_rest/oauth.py`, `httpx`):
   - segue `publishedoauth.json`: Basic `ClientID:ClientSecret`, `x-app-key`, `X-Request-Id` (GUID), `enterpriseId` só com `client_credentials`, corpo em formulário;
   - validade pelo `expires_in` (padrão 3600 s) ou pelo `exp` do JWT, o que vencer antes (sem validar assinatura: serve só para agendar a renovação);
   - 400/401/403 → `AuthRejectedError` (alerta, sem repetição rápida); rede, 429 e 5xx → `AuthUnavailableError` (backoff);
   - nada de segredo em log, `repr` ou mensagem de erro;
   - `credentials_from_settings` mapeia `OHIP_AUTH_MODE` (`resource_owner` → grant `password`).
3. **Lease** (`application/use_cases/lease.py`, `adapters/oracle/lease_store.py`):
   - `LeaseKeeper` adquire, renova a cada `LEASE_RENEW_INTERVAL_S` e libera, com o SQL do ADR-0008 e o relógio do banco.
   - Outro dono → nova tentativa depois de `ttl` + jitter (`LEASE_ACQUIRE_JITTER_S`).
   - Renovação recusada → perdido na hora.
   - Banco sem resposta → continua tentando, mas a partir do **prazo local** (`ttl` contado do início da última renovação bem-sucedida) o epoch deixa de valer e o processo para de gravar.
   - Só libera se ainda for o dono.
   - Linha ausente → `LeaseNotProvisionedError` (erro de provisionamento; a aplicação nunca cria a linha).
   - A posse de verdade é sempre conferida pela barreira de epoch no banco.
4. **`LeaseSettings`** (`LEASE_TTL_S`, `LEASE_RENEW_INTERVAL_S`, `LEASE_ACQUIRE_JITTER_S`): grupo comum a consumer e publisher, previsto no ADR-0010. **Substitui** `CONSUMER_LEASE_TTL_S` e `CONSUMER_LEASE_RENEW_INTERVAL_S`, que ainda não estavam em uso em nenhum ambiente.
5. **Redis** (`adapters/redis/`):
   - `RedisSeenCache` (`ohip:seen:*`, MGET e pipeline com TTL);
   - `RedisQueueDedup` (`ohip:processed:*`, enricher);
   - `RedisTokenCache`;
   - `InMemoryMetrics` + `RedisMetricsPublisher` (`ohip:metrics:<processo>:<instância>`, TTL 60 s).
   - Cliente com timeouts de 2 s. As falhas sobem como exceção e os casos de uso as tratam como "sem atalho": Redis nunca afeta a correção.
   - O protocolo `RedisLike` segue as assinaturas reais do redis-py (o mypy confere com o cliente real nos testes `@redis`).
6. **Testes**: `respx` para o OAuth; Redis falso para os adapters; fake de lease sobre os mesmos epochs da barreira; cenários de lease em `tests/stores` (fake e Oracle); testes `@redis` com `TEST_REDIS_URL` e `TEST_REDIS_DISPOSABLE=sim`.

## Consequências

- O consumer da Fase 5 compõe: `TokenProvider` → `connection_init`; `LeaseKeeper` → epoch da barreira e `DRAINING` ao perder; `RedisSeenCache` → `ProcessEventBatch`.
- A renovação do lease e o token dependem do relógio local só para medir intervalos; prazos absolutos vêm do banco (lease) ou do emissor (token).
- Variáveis novas: `LEASE_*` (`.env.example`).
