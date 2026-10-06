# ADR-0020 — Endurecimento e operação (Fase 10)

- Status: Proposto (Fase 10)
- Data: 2026-10-05

## Contexto

As Fases 0–9 entregaram os cinco processos (`ohip-consumer`, `ohip-publisher`, `ohip-enricher`, `ohip-api`, `ohip-admin`). Faltam, para operar (PLAN, Fase 10):

- o consumer gravar em `OHIP_CONSUMER_STATUS` o que a API já lê e devolve `null`/0 (pendência do ADR-0017): `subscription_id`, `last_message_at`, reconexões, falhas seguidas, próxima tentativa, `token_expires_at`, ping/pong e RTT;
- o expurgo do ARCHITECTURE §5.1;
- o teste de carga a 10× o pico (RNF-13);
- unidades systemd, configuração do Nginx e regras de alerta;
- o diagnóstico `/entender` do código inteiro.

Q-6 (VMs), Q-7 (ferramenta de alertas) e Q-8 (pico) seguem sem resposta: vale o padrão de cada uma (OPEN_QUESTIONS). Q-17 (Oracle de teste) também segue aberta: nada aqui roda contra banco real, e nenhuma DDL é executada.

## Decisão

### 1. Status do consumer (`OHIP_CONSUMER_STATUS`)

Sem mudança de tabela nem de API: as colunas já existem (`sql/002`) e o contrato do `GET /api/v1/status` já as prevê (docs/API.md). O port `ConsumerStatusStore` ganha:

- `record_subscribed(chain, instance_id, subscription_id, token_expires_at)`: `state = SUBSCRIBED`, `subscription_id`, `connected_at` (relógio do banco), `token_expires_at` e `next_attempt_at = NULL`. Substitui o `record_state(SUBSCRIBED)`;
- `record_health(chain, ConnectionHealth)`: `last_message_at`, `last_ping_at`, `last_pong_at` e `last_rtt_ms`. Só as colunas com valor entram no `UPDATE` (sem `NULL` sobrescrevendo dado bom). Horários do relógio do processo (UTC): são informativos e não entram na regra dos 10 s;
- `record_disconnect(..., consecutive_failures, reconnect, next_attempt_in_s)`: grava também as falhas seguidas, soma 1 em `reconnects` quando a decisão é reconectar, e `next_attempt_at = horário do banco + espera` (ou `NULL` ao parar).

A sessão grava a saúde **pela tarefa de controle**, a cada `CONSUMER_STATUS_INTERVAL_S` (padrão 15 s), e uma última vez ao terminar. Nunca pela leitura nem pelo heartbeat: uma escrita lenta no Oracle não pode atrasar `pong` nem a leitura de frames. Uma linha por chain a cada 15 s é carga desprezível. Falha ao gravar status só gera aviso (é informativo; não envenena a conexão). A última gravação de saúde acontece **depois** de fechar o socket (um Oracle lento não atrasa o fechamento).

**Sem lease, nada é gravado na linha da chain** (regra da Fase 5, agora também no fim da sessão): se o lease foi perdido (renovação recusada) ou a barreira de epoch acusou outra dona (`LeaseLostError` na gravação ou no retry de DLQ), a sessão não grava `DRAINING`, saúde nem desconexão. Na troca ativo/passivo (Q-6), a antiga, ainda drenando, sobrescreveria a linha da nova (`WAITING` sobre `SUBSCRIBED`) e o alerta "fora de `SUBSCRIBED`" dispararia à toa.

`consecutive_failures` passa a ter o mesmo significado no banco e no processo: falhas seguidas, zeradas por uma sessão saudável (recebeu evento ou ficou assinada `healthy_after_s`). O comentário da coluna no `sql/002` é ajustado (só comentário).

### 2. Expurgo (`ohip-purge`, ARCHITECTURE §5.1)

- Caso de uso `PurgeExpiredData` (`application/use_cases/purge.py`), port `PurgeStore`, adapter `adapters/oracle/purge_store.py`, processo `ohip-purge` (`entrypoints/purge.py`) disparado por timer do systemd (diário, `Persistent=true`).
- Ordem fixa, pelas chaves estrangeiras: (1) `OHIP_DLQ` resolvida antes do corte; (2) `OHIP_OUTBOX` `SENT` antes do corte e sem item de DLQ apontando para ela; (3) `OHIP_OUTBOX` `FAILED` criada antes do corte e já sem item de DLQ: o item `PUBLISH` foi resolvido (o retry cria linha nova; o descarte não) e expurgado no passo 1, e sem este passo a linha antiga e o bruto ficariam para sempre; (4) `OHIP_EVENT_RAW` recebido antes do corte, **sem** linha na outbox **nem** na DLQ (`NOT EXISTS`). Linhas `PENDING`, `FAILED` com DLQ aberta e DLQ aberta seguram o bruto.
- Corte = `SYS_EXTRACT_UTC(SYSTIMESTAMP) - NUMTODSINTERVAL(:days, 'DAY')` (relógio do banco), `PURGE_RETENTION_DAYS` (padrão 90, Q-9).
- Lotes: `DELETE ... AND ROWNUM <= :n` com commit por lote (`PURGE_BATCH_SIZE`, padrão 5000), pausa entre lotes (`PURGE_PAUSE_MS`) e prazo total (`PURGE_MAX_RUNTIME_S`). Prazo vencido ou SIGTERM: termina o lote em curso e sai com sucesso; o resto fica para a próxima execução (cada lote é independente e idempotente).
- `PURGE_DRY_RUN=true` (**padrão**): só conta (`SELECT COUNT(*)` com os mesmos filtros) e não apaga nada; apagar exige `false`, depois de conferir as contagens. Cada passo conta pelo estado atual (o bruto liberado pela outbox só aparece na execução seguinte). `PURGE_RETENTION_DAYS` tem mínimo de 30: um valor errado apagaria histórico recente.
- Credenciais: o mesmo grupo `ORACLE_*`, num arquivo de ambiente próprio com o usuário `ohip_purge` (`sql/900_grants_example.sql`: `SELECT, DELETE` só nas três tabelas). Sem Redis nem RabbitMQ.
- Usa só os índices que já existem (`received_at`, `sent_at`, `resolved_at`, `event_raw_id`, `outbox_id`). Plano de execução a conferir no Oracle real (checklist da Q-17).
- Tabelas de domínio (após a Q-1) **não podem** ter chave estrangeira para `OHIP_EVENT_RAW`: guardam `source_event_raw_id` só como rastreio, e o bruto é expurgado antes delas.
- Resultado no log (`expurgo_concluido` com as contagens por tabela). Saída 1 em erro; a unit traz `OnFailure=` comentado até a Q-7 definir a notificação.

### 3. Teste de carga (RNF-13)

- `tests/load/` (marcador `load`, fora do `make check`), alvo `make test-load`. Consumer real (`ChainConsumer` e `WebsocketsConnector`) contra o `FakeOhipServer` em localhost, com envio ritmado (`Behavior.rate_per_s`).
- Persistência: o fake em memória com **latência simulada do Oracle** numa thread (como o executor de 1 thread do adapter real): `LOAD_DB_BATCH_MS` por lote + `LOAD_DB_EVENT_MS` por evento.
- Cenários: (a) **sustentado a 10× o pico da Q-8** (300/min/chain → 50 eventos/s) por `LOAD_DURATION_S` (padrão 60 s); (b) **rajada** (fechamento do dia: `LOAD_BURST_EVENTS`, padrão 10 000, de uma vez), que exercita a fila interna cheia (espera, nunca descarta).
- Mede a latência por evento do envio pelo servidor ao commit, em p50/p95/p99, além de eventos/s e do pico de RSS.
- Orçamentos: p95 ≤ 5 s; nenhum evento perdido nem duplicado; sem reconexão nem envenenamento; crescimento do pico de RSS ≤ `LOAD_MEMORY_GROWTH_MB` (padrão 20 MB) do primeiro quarto ao fim (pico de RSS em vez de `tracemalloc`, que distorceria a latência). O fake não guarda as linhas gravadas, para a memória medida ser a do consumer.
- **Medição (2026-10-05, Mac de desenvolvimento, Oracle simulado a 100 ms por lote + 1 ms por evento):** sustentado 50/s por 60 s → p50 0,31 s, p95 0,46 s, p99 0,46 s, RSS +3,6 MB; rajada de 10 000 → ~500 eventos/s, tudo gravado em 19,8 s, sem reconexão (p95 12 s: é a fila esvaziando, fora do orçamento do sustentado). Dentro do orçamento: o Otimizador não foi acionado.
- O Otimizador só atua se um orçamento estourar. Limite: o banco é simulado; a medição com Oracle 11g real entra no checklist da Q-17.

### 4. systemd (`deploy/systemd/`)

- Unidades: `ohip-consumer@.service` (instância = chain), `ohip-publisher.service`, `ohip-enricher@.service` (instâncias 1..N), `ohip-api.service`, `ohip-admin.service`, `ohip-purge.service` + `ohip-purge.timer` e `ohip-streaming.target` (agrupa).
- Todas: `User=ohip`, `EnvironmentFile=` em `/etc/ohip/` (fora do repositório, `0640 root:ohip`), saída JSON no journald (Q-16), e endurecimento (`NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, `PrivateDevices`, `RestrictAddressFamilies`, `CapabilityBoundingSet=` vazio).
- Consumer: `Restart=always`, **`RestartSec=10`** (regra dos 10 s, ADR-0007) e `TimeoutStopSec=90` (drenagem + liberação do lease). A chain vem do nome da instância com `%I` (sem o escape do systemd, que transformaria espaço em `\x20`) e vence o arquivo de ambiente (`consumer-%i.env`). Publisher e enricher: `RestartPreventExitStatus=2` (topologia divergente exige ação humana). API e painel ouvem só em `127.0.0.1`.
- O painel nunca recebe `ORACLE_*`/`REDIS_*` (arquivo de ambiente próprio, ADR-0004).

### 5. Nginx (`deploy/nginx/ohip-streaming.conf`)

- TLS; `/api/` → `127.0.0.1:8080`; `/admin/` → `127.0.0.1:8081`.
- `/health`, `/ready` e `/metrics` só da rede de monitoramento (`allow`/`deny`); `/docs` e `/openapi.json` bloqueados.
- No painel: login no Nginx (Q-5; o modelo usa `auth_basic` como exemplo, trocar pelo SSO). O Nginx **sobrescreve** `X-Forwarded-User`/`X-Forwarded-Groups` (o cliente nunca os define) e envia o segredo `X-Admin-Proxy-Secret` (ADR-0018). O grupo sai de um mapa explícito (`groups.map`) com padrão **vazio**: autenticar não basta para ler (com SSO corporativo, a empresa inteira autentica).
- Cabeçalhos de segurança e `client_max_body_size` pequeno.

### 6. Alertas (Q-7, padrão: Prometheus)

`deploy/prometheus/ohip-alerts.yml` com as regras do RF-11 sobre as métricas do `/metrics`. Contadores vêm de snapshots por processo, com o rótulo `instance` novo a cada restart, e só aparecem depois do primeiro incremento: uma série que nasce com 1 dá `increase() = 0`. Por isso: (a) cada processo registra com 0, na partida, os contadores citados nas regras (`alert_counters()` nos entrypoints); (b) as regras de eventos raros (cartão, lease perdido, conexão envenenada, disjuntor e erro inesperado do enricher) somam `X > 0 unless X offset W`, que pega a série nascida com valor; (c) cada processo grava um último snapshot ao sair (o lease perdido costuma ser seguido da saída). Regras: consumer `STOPPED`, conexão fora de `SUBSCRIBED` > Y min, sem mensagem > X min, falhas seguidas, 4409 repetido (3 ou mais em 30 min, marcado `# AJUSTAR` para revisão com a Q-7; substitui o `LOCK_ALERT_AFTER` do ADR-0007, que não foi criado), DLQ aberta, outbox parada, fonte de métricas fora, alertas críticos do enricher. Limites marcados `# AJUSTAR` até a resposta da Q-7/Q-8. Com outra ferramenta, as mesmas expressões servem de referência.

### 7. Verificações automáticas

`tests/contract/test_deploy_files.py` lê as unidades e a configuração do Nginx e confere as regras acima: `RestartSec=10`, `%I`, `RestartPreventExitStatus=2`, comandos que existem em `[project.scripts]`, nenhum segredo no repositório, headers de identidade sobrescritos, mapa de grupos sem padrão, endpoints internos bloqueados, métricas dos alertas existentes, contadores dos alertas registrados com 0 e regras de eventos raros com a primeira ocorrência coberta.

### 8. `/entender` geral

O Revisor faz o diagnóstico do código inteiro (`.claude/commands/entender.md`), só leitura. Refatorações só depois da aprovação humana, com testes de caracterização (RNF-15), fora desta entrega.

## Consequências

- O status da API deixa de mostrar `null` nos campos que dependiam do consumer.
- Novos grupos de configuração: `CONSUMER_STATUS_INTERVAL_S` e `PURGE_*`. Novo script: `ohip-purge`.
- O expurgo exige usuário próprio no Oracle (DBA da Aviva) e só deve apagar depois de um `PURGE_DRY_RUN=true` conferido.
- Sem dependência nova: Prometheus, Nginx e systemd são infraestrutura do ambiente, não bibliotecas do projeto.
