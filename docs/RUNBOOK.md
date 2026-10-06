# Runbook — ohip-streaming

> Status: **Fase 10** (ADR-0020): deploy com systemd e Nginx, expurgo, teste de carga e alertas. Pendências de ambiente: Q-5 (login do painel), Q-6 (VMs), Q-7 (ferramenta de alertas), Q-8 (pico real) e Q-17 (Oracle de teste).

## Pré-requisitos (manuais, antes do primeiro deploy)

1. Ambiente OPERA Cloud ≥ 22.3.0.1 com **Streaming Enabled** no Developer Portal (cliente: workshop B93152 para a primeira chain UAT; demais via SR técnico).
2. Aplicação registrada no OHIP com os eventos da fase 1 **aprovados** pelo dono do ambiente. Uma app key separada por ambiente e outra para desenvolvimento.
3. `clientId`, `clientSecret`, app key, gateway URL e, se Client Credentials, `enterpriseId` — no cofre/ambiente, nunca no repositório.
4. Se o ambiente usar OCIM: vida do token OAuth ≥ 60 min.
5. VM Linux com saída `wss://` 443 para o gateway OHIP, **sem inspeção TLS**, idle timeout de NAT/firewall > 15 s.
6. Oracle Instant Client 19 instalado; schema e usuários conforme `sql/` (aplicado pelo DBA).
7. RabbitMQ e Redis acessíveis pela VM.
8. Usuário `ohip_purge` no Oracle só com `SELECT, DELETE` em `OHIP_DLQ`, `OHIP_OUTBOX` e `OHIP_EVENT_RAW`, e sinônimos (`sql/900_grants_example.sql`; aplicado pelo DBA).

## Instalação na VM (systemd + Nginx, ADR-0020)

1. Usuário de serviço e código:

   ```bash
   sudo useradd --system --home /var/lib/ohip --shell /usr/sbin/nologin ohip
   sudo git clone <repo> /opt/ohip-streaming && cd /opt/ohip-streaming
   sudo poetry install --only main      # .venv em /opt/ohip-streaming/.venv (poetry.toml)
   ```

2. Instant Client 19 no Linux: registre o diretório no `ldconfig` (`/etc/ld.so.conf.d/oracle-instantclient.conf` + `sudo ldconfig`) e deixe `ORACLE_CLIENT_LIB_DIR` vazio. Segundo a documentação do python-oracledb, no Linux as bibliotecas precisam estar no caminho do sistema antes de o processo subir. As units deixam o disco só leitura (`ProtectSystem=strict`), exceto `/var/lib/ohip` (`HOME` do serviço): confira na primeira subida se o Instant Client grava `sqlnet.log`/`oradiag_*` ali; se tentar gravar no diretório de trabalho, aponte `TNS_ADMIN`/`ADR_BASE` (ou `LOG_DIRECTORY_CLIENT` no `sqlnet.ora`) para `/var/lib/ohip` (checklist da Q-17).
3. Arquivos de ambiente em `/etc/ohip/` (`0640 root:ohip`, fora do repositório; modelos no `.env.example`):

   | Arquivo | Usado por | Conteúdo |
   | --- | --- | --- |
   | `common.env` | consumer, publisher, enricher, API | `APP_*`, `LOG_*`, `ORACLE_*` (usuário `ohip_app`), `REDIS_*`, `RABBITMQ_*` |
   | `consumer-<CHAIN>.env` | `ohip-consumer@<CHAIN>` | `OHIP_*` da app da chain, `CONSUMER_*`, `LEASE_*` (a chain vem do nome da instância) |
   | `publisher.env`, `enricher.env` | publisher, enricher (opcionais) | `PUBLISHER_*`, `ENRICHER_*` (e `OHIP_*` se `ENRICHER_REST_ENABLED=true`) |
   | `api.env` | `ohip-api` | `API_*` |
   | `admin.env` | `ohip-admin` | só `ADMIN_*`, `APP_*`, `LOG_*` (**nunca** `ORACLE_*`/`REDIS_*`, ADR-0004) |
   | `purge.env` | `ohip-purge` | `APP_*`, `LOG_*`, `ORACLE_*` com o usuário `ohip_purge`, `PURGE_*` |

4. Units e timer (`deploy/systemd/`):

   ```bash
   sudo cp deploy/systemd/* /etc/systemd/system/ && sudo systemctl daemon-reload
   sudo systemctl enable --now ohip-streaming.target ohip-consumer@CHAIN1 ohip-enricher@1
   systemctl list-dependencies ohip-streaming.target
   journalctl -u 'ohip-*' -o cat -f     # logs JSON (Q-16: o coletor lê do journald)
   ```

   - `ohip-consumer@<CHAIN>`: a chain vem do nome da instância (`%I`, sem escape) e vence o arquivo de ambiente; `RestartSec=10` (regra dos 10 s, ADR-0007), nunca desiste de reiniciar, `TimeoutStopSec=90` para drenar e liberar o lease.
   - `ohip-publisher` e `ohip-enricher@N`: `RestartPreventExitStatus=2` (topologia divergente exige ação humana; `systemctl restart` depois de corrigir).
   - Mais enrichers: `systemctl enable --now ohip-enricher@2`.
   - Segunda VM (Q-6): as mesmas units; o lease no Oracle deixa a segunda passiva (consumer e publisher).
5. Nginx (`deploy/nginx/ohip-streaming.conf`, incluído no bloco `http`): ajuste os pontos marcados `AJUSTAR` e crie em `/etc/nginx/ohip/` (`0640 root:nginx`): `htpasswd` (login do painel até a Q-5), `groups.map` (`usuario ohip-admin;` ou `usuario ohip-read;` por linha; quem não estiver nele leva 403) e `admin-proxy-secret.conf` (`proxy_set_header X-Admin-Proxy-Secret "<o mesmo ADMIN_PROXY_SECRET>";`). Depois `sudo nginx -t && sudo systemctl reload nginx`.
6. Prometheus (padrão da Q-7): job `ohip-api` lendo `https://<host>/metrics` a partir da rede liberada no Nginx, e as regras de `deploy/prometheus/ohip-alerts.yml` (limites `AJUSTAR` com a Q-7/Q-8).

`tests/contract/test_deploy_files.py` confere esses arquivos contra o código no `make check` (scripts, `RestartSec`, headers do painel, endpoints internos, métricas dos alertas e ausência de segredos).

## Testes contra o Oracle de teste (Fase 3)

Os cenários de `tests/stores/` rodam no fake e, com estas variáveis, num Oracle 11g **de teste**:

1. O DBA cria um schema **descartável** e aplica `sql/001` a `003` (o `publisher` precisa existir em `OHIP_LEASE`). Os testes não rodam DDL; eles apagam e semeiam só as linhas das chains `ZZT1`/`ZZT2`.
2. Numa máquina x86_64 com Instant Client 19 (não existe para Mac arm64):

   ```bash
   export TEST_ORACLE_DSN=host:1521/servico TEST_ORACLE_USER=ohip_test TEST_ORACLE_PASSWORD='***'
   export TEST_ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_19_x TEST_ORACLE_DISPOSABLE_SCHEMA=sim
   make test-integration
   ```

   Sem `TEST_ORACLE_DISPOSABLE_SCHEMA=sim` os testes de Oracle são pulados. **Nunca** aponte para homologação ou produção.

## Consumer (`ohip-consumer`)

- Um processo por chain, nunca dentro de Uvicorn/Gunicorn: `systemctl start ohip-consumer@<CHAIN>` (ou `ohip-consumer` com `OHIP_CHAIN_CODE`).
- SIGTERM: `complete` → drena → grava a desconexão → libera o lease → sai. Não use `kill -9` (o próximo start espera 10 s inteiros, mas nada se perde).
- Estado em `OHIP_CONSUMER_STATUS`: `SUBSCRIBED` = recebendo; `WAITING` = aguardando reconexão; `DRAINING` = encerrando a assinatura; `STOPPED` = **ação humana** (4403/4406 ou mensagem grande demais repetida); o processo fica parado até ser reiniciado depois da correção.
- A mesma linha traz `subscription_id` e `connected_at` (da assinatura), `token_expires_at`, `last_message_at`, `last_ping_at`/`last_pong_at` e `last_rtt_ms` (gravados a cada `CONSUMER_STATUS_INTERVAL_S`, padrão 15 s, e ao fim da conexão), `reconnects` (acumulado), `consecutive_failures` (zera depois de uma sessão saudável) e `next_attempt_at` (horário do banco da próxima tentativa). Falha ao gravar esses campos só gera `status_indisponivel` (aviso): a conexão segue.
- Logs úteis: `ohip_assinado` (subscription_id, offset: informe ao suporte Oracle), `conexao_envenenada` (fila travada ou banco fora: reconecta pelo offset confirmado), `ohip_sem_prova_de_vida`, `ohip_assinatura_encerrada` (frame `error` do servidor, D-10), `ws_handshake_recusado` (HTTP 400: hash da app key ou URL), `consumer_alerta`/`consumer_parado`.

## Publisher (`ohip-publisher`)

- Um ativo por ambiente (lease `publisher`); uma instância extra fica esperando o lease. Comando: `ohip-publisher`.
- Topologia nossa (declarada na partida): `ohip.events` (topic, com alternate exchange), `ohip.events.unrouted` → fila `ohip.unrouted` (limite `RABBITMQ_UNROUTED_MAX_LENGTH`, descarta as mais antigas), `ohip.reprocess` (fanout) → `ohip.enricher`. Cada time declara a própria fila e a binding em `ohip.events` (Q-13).
- Mensagens em `ohip.unrouted`: alguém publica um evento que nenhuma fila assinou. Crie a binding certa ou confirme que o evento pode ser ignorado.
- Saída com código 2 e `publisher_topologia_divergente`: um exchange ou fila nossa já existe no broker com outros argumentos, ou o usuário não tem permissão de declarar. Corrija no broker (ou ajuste a configuração) antes de reiniciar. A unit do systemd usa `RestartPreventExitStatus=2` para não reiniciar em laço.
- `broker_indisponivel` repetido: broker fora ou rede; nenhuma tentativa é gasta e nada vai para a DLQ, por mais que dure. Linhas `FAILED` + DLQ `PUBLISH` só por `nack` ou mensagem grande demais; reprocesse pela API.
- Testes contra um RabbitMQ de teste (vhost descartável): `TEST_RABBITMQ_URL` + `TEST_RABBITMQ_DISPOSABLE=sim` e `make test-integration`.

## API de controle (`ohip-api`)

- Comando: `ohip-api` (Uvicorn com `API_WORKERS` workers em `API_HOST`:`API_PORT`, padrão `127.0.0.1:8080`, atrás do Nginx). Cada worker tem o seu pool Oracle (`ORACLE_POOL_MAX` conexões); se o Oracle estiver fora na partida, o worker não sobe e o systemd tenta de novo.
- Tokens de serviço: gere um token aleatório por perfil (`read` e `admin`), guarde o token no cofre do painel e ponha só o hash em `API_SERVICE_TOKENS` (`{"<sha256>": "read", "<sha256>": "admin"}`):

  ```bash
  python -c 'import secrets; print(secrets.token_urlsafe(32))'   # token (vai para o painel)
  printf '%s' "$TOKEN" | sha256sum | cut -d' ' -f1                 # hash (vai para a API)
  ```

- `/health` (processo de pé), `/ready` (Oracle, Redis e RabbitMQ; 503 com o resultado de cada um) e `/metrics` (Prometheus) não pedem token: o Nginx não pode expô-los para fora.
- Ações `admin` exigem `X-Actor` (o painel repassa o usuário). Toda requisição gera `api_requisicao` (método, rota sem query string, status, duração, perfil, ator e `request_id`); `?unmasked=true` gera `auditoria_dado_sem_mascara`.
- Erro 503 `DEPENDENCY_UNAVAILABLE`: Oracle fora ou pool esgotado (`ORACLE_POOL_WAIT_TIMEOUT_MS`). Erro 500: procure o `request_id` da resposta nos logs (`api_erro_inesperado`).
- Prazo de cada consulta da API ao Oracle: `API_ORACLE_CALL_TIMEOUT_MS` (padrão 15 s, separado do consumer). Estourou → 503; consultas lentas recorrentes: veja o plano de execução das listagens (checklist da Q-17).
- Em produção, `/docs`, `/redoc` e `/openapi.json` ficam desligados.
- `/metrics` lê os snapshots dos processos pelo conjunto `ohip:metrics_index` (sem varrer o Redis).
- `GET /api/v1/status` vem do cache do Redis (`ohip:api:status`, 5 s), com os campos que o consumer grava em `OHIP_CONSUMER_STATUS` (ADR-0020 §1).

## Enricher (`ohip-enricher`)

- Comando: `ohip-enricher`; N instâncias em paralelo (sem lease: o `MERGE` condicional garante a ordem). **Sem regras até a Q-1**: todo evento termina `UNMAPPED` (métrica `ohip_events_unmapped_total` por `eventName`, útil para dimensionar a Q-1).
- Fila `ohip.enricher` (declarada igual ao publisher). Routing keys de `ohip.events` em `ENRICHER_BINDINGS` (vazio até a Q-1); reprocessamentos chegam pelo `ohip.reprocess`. **Com `ENRICHER_BINDINGS` vazio nenhum evento novo chega e `ohip_events_unmapped_total` fica zerada**: para medir o volume por `eventName` antes da Q-1, use `ENRICHER_BINDINGS=ohip.#` (tudo termina `UNMAPPED`; carga extra no Oracle de um `UPDATE` por evento). O enricher só **cria** bindings: tirar uma key da variável não a remove do broker. Para desligar, remova à mão (`rabbitmqctl` ou console: binding `ohip.events` → `ohip.enricher` com a key) e reinicie. Saída com código 2 e `enricher_fila_divergente`: a fila existe com outros argumentos (a unit precisa de `RestartPreventExitStatus=2`).
- REST do OHIP só com `ENRICHER_REST_ENABLED=true` (Q-2); aí o enricher precisa das variáveis `OHIP_*` (credenciais da `OHIP_CHAIN_CODE`). Logs `ohip_rest_chamada` trazem o `request_id` enviado em `x-request-id` (informe ao suporte Oracle).
- `enricher_dependencia_indisponivel`: Oracle, REST (5xx, 429, token) ou rede fora; a mensagem volta para a fila sem gastar tentativa e o processo espera (backoff, ou o `Retry-After` do 429). `enricher_mensagem_na_dlq`: a mensagem falhou `ENRICHER_MAX_ATTEMPTS` vezes; item `NORMALIZE`/`ENRICH` na DLQ, bruto `FAILED`; reprocesse pela API ou pelo painel depois de corrigir. O texto do erro na DLQ e no log só traz mensagens nossas; de exceção de outra origem (ex.: bug numa regra) fica só a classe e `detalhe omitido (pode conter dados pessoais)`.
- **`enricher_falha_sistemica` (crítico)**: `ENRICHER_SYSTEMIC_FAILURE_THRESHOLD` mensagens seguidas falharam igual (mesmo estágio e classe) sem nenhum sucesso. É problema do sistema, não das mensagens: REST 403/400 (app sem assinatura da API no Developer Portal, `ENRICHER_RESOURCE_PATHS` errado), tabela de domínio sem grant ou inexistente (ORA-00942/01031), regra com bug. As mensagens seguintes com essa falha voltam para a fila com backoff já na primeira tentativa (métrica `ohip_enricher_systemic_failures_total`), em vez de irem para a DLQ. Corrija a causa: o primeiro sucesso do mesmo caminho fecha o disjuntor (`enricher_falha_sistemica_encerrada`): um evento normalizado/enriquecido, ou um `UNMAPPED` se a falha era sem regra. Evento sem regra não fecha disjuntor aberto por falha de regra. As mensagens que foram para a DLQ antes do limite: retry pela API ou pelo painel. Limite conhecido: com várias regras, a falha de uma só intercalada com sucessos de outra regra não abre o disjuntor (acompanhe `ohip_enricher_dlq_total` por estágio).
- **`ohip_rest_credenciais_recusadas` (crítico)**: o OAuth (ou a REST, com token novo) recusou `ENRICHER_REST_AUTH_REJECTED_ALERT` vezes seguidas (crítico uma vez; cada recusa gera aviso `ohip_rest_credencial_recusada` e conta em `ohip_rest_auth_rejected_total`). O enricher continua tentando no backoff máximo (um pedido de token por ciclo): confira as credenciais `OHIP_*` (exige reiniciar) ou a assinatura no Developer Portal (não exige). A contagem zera quando a REST aceita um token.
- **`enricher_erro_inesperado` (crítico)**: bug nosso no tratamento da mensagem. O processo não cai (cairia de novo ao receber a mesma mensagem); a mensagem volta para a fila com backoff (`ohip_enricher_unexpected_errors_total`). O log traz a classe e o local (`where`), sem o texto da exceção. Abra incidente para o time do serviço.

## Painel (`ohip-admin`)

- Comando: `ohip-admin` (Gunicorn com `ADMIN_WORKERS` workers síncronos em `ADMIN_HOST`:`ADMIN_PORT`, padrão `127.0.0.1:8081`). Fala só com a API (`ADMIN_API_BASE_URL`); nunca recebe credenciais do Oracle, do Redis ou do OHIP.
- Tokens: `ADMIN_API_READ_TOKEN` e `ADMIN_API_ADMIN_TOKEN` são os tokens **em claro** cujo SHA-256 está em `API_SERVICE_TOKENS` da API (seção acima). `ADMIN_SECRET_KEY`: 32+ caracteres aleatórios (`python -c 'import secrets; print(secrets.token_urlsafe(48))'`); trocar a chave derruba as sessões abertas (os formulários abertos precisam ser recarregados).
- Login (Q-5, padrão): o Nginx autentica e **sobrescreve** `X-Forwarded-User` e `X-Forwarded-Groups` (grupos separados por vírgula) em toda requisição para `/admin`, inclusive apagando o que vier do navegador. Grupo `ADMIN_ADMIN_GROUP` (`ohip-admin`) = perfil admin; `ADMIN_READ_GROUP` (`ohip-read`) = leitura; outros = 403. Sem o header de usuário, o painel responde 401 (Nginx mal configurado).
- O Nginx precisa **sobrescrever** os dois headers em toda requisição (`proxy_set_header X-Forwarded-User $usuario; proxy_set_header X-Forwarded-Groups $grupos;`, inclusive vazios), e o usuário precisa ser ASCII com até 100 caracteres (senão o painel mostra 400 "Usuário não suportado").
- Painel fora do endereço local (`ADMIN_HOST` ≠ `127.0.0.1`/`::1`): obrigatório `ADMIN_PROXY_SECRET` (32+ caracteres), enviado pelo Nginx em `X-Admin-Proxy-Secret` (`proxy_set_header`). Mesmo local, o segredo é recomendado: impede outros processos da VM de chamar o painel com headers forjados.
- Login vencido: o Nginx deve responder 401 (não 302) quando a requisição tiver `HX-Request: true`; o bloco para de atualizar e o "atualizado há" denuncia. O painel, se receber a requisição sem usuário, pede ao navegador para recarregar a página (`HX-Refresh`).
- HTMX: `static/htmx.min.js` (2.0.11) vai junto com o código, com o hash publicado em htmx.org (`static/HTMX_PROVENANCE.md`); o teste do painel confere o arquivo. Para atualizar, troque o arquivo e o `HTMX_INTEGRITY` em `admin/templating.py` com o hash publicado da nova versão.
- Página 503 "A API de controle não respondeu": confira `ohip-api` (`/ready`). Página 502 "A API recusou o token do painel": os tokens do painel não batem com os hashes da API.
- Logs: `painel_requisicao` (rota sem query string, usuário, perfil, `request_id`, que também vai para a API em `X-Request-ID`), `painel_erro_api`, `painel_csrf_recusado`.

## Expurgo (`ohip-purge`)

- Timer diário (`ohip-purge.timer`, 03:30 + até 15 min; `Persistent=true` roda ao ligar se perdeu a janela). Manual: `sudo systemctl start ohip-purge` e `journalctl -u ohip-purge -o cat`.
- Ordem (ARCHITECTURE §5.1): `OHIP_DLQ` resolvida → `OHIP_OUTBOX` `SENT` → `OHIP_OUTBOX` `FAILED` já sem item de DLQ (o item `PUBLISH` foi resolvido e expurgado) → `OHIP_EVENT_RAW` sem outbox nem DLQ. Linhas `PENDING`, `FAILED` com DLQ aberta e DLQ aberta seguram o bruto. Corte: relógio do banco − `PURGE_RETENTION_DAYS` (padrão 90, mínimo 30; Q-9).
- **Primeira execução em cada ambiente**: deixe `PURGE_DRY_RUN=true` (padrão), confira `expurgo_simulado` (contagem por tabela) no journal e só então ponha `false`. No dry run cada passo conta pelo estado atual: o bruto liberado pela outbox só aparece na execução seguinte.
- Lotes de `PURGE_BATCH_SIZE` com commit por lote e pausa `PURGE_PAUSE_MS`; para em `PURGE_MAX_RUNTIME_S` ou no SIGTERM depois do lote em curso (`expurgo_interrompido`; o resto fica para o dia seguinte). `expurgo_concluido` traz as linhas por tabela e `complete`.
- Erro (saída 1, `expurgo_falhou`): os lotes já confirmados ficam; corrija e rode de novo. ORA-01031/00942: grants ou sinônimos do `ohip_purge`.
- Tabelas de domínio (após a Q-1) não podem ter chave estrangeira para `OHIP_EVENT_RAW` (ADR-0020 §2).

## Teste de carga (RNF-13)

- `make test-load` (fora do `make check`): consumer real contra o servidor OHIP simulado, banco em memória com a latência do Oracle simulada numa thread (`LOAD_DB_BATCH_MS`, `LOAD_DB_EVENT_MS`). Cenários: sustentado a 10× o pico da Q-8 (`LOAD_RATE_PER_S`, padrão 50/s por chain, por `LOAD_DURATION_S`) e rajada de fechamento do dia (`LOAD_BURST_EVENTS`). `LOAD_REPORT=arquivo.jsonl` grava os números.
- Orçamentos: p95 (envio → commit) ≤ 5 s no sustentado, nada perdido nem duplicado, sem reconexão e memória estável.
- Medição de 2026-10-05 (Mac de desenvolvimento, Oracle simulado a 100 ms por lote + 1 ms por evento): sustentado 50/s por 60 s → p50 0,31 s, **p95 0,46 s**, p99 0,46 s, RSS +3,6 MB; rajada de 10 000 eventos → ~500 eventos/s, tudo gravado em 19,8 s, sem reconexão nem envenenamento (p95 12 s na rajada: é a fila esvaziando). Orçamento atendido: o Otimizador não foi acionado.
- Refazer com o Oracle 11g real (checklist da Q-17): medir o tempo de um lote de 200 eventos no adapter, ajustar `LOAD_DB_*` e rodar de novo; com a Q-8 respondida, ajustar `LOAD_RATE_PER_S`.

## Hash da app key

```bash
echo -n "$OHIP_APP_KEY" | sha256sum | cut -d' ' -f1   # hex minúsculo
```

## Códigos de fechamento (resumo — ADR-0007)

| Código | O que fazer |
| --- | --- |
| 4401 | Automático (novo token). Se repetir: conferir credenciais e hash da app key. |
| 4403 | **Ação humana**: conferir chain, habilitação de streaming e aprovação dos eventos no Developer Portal. Depois `systemctl restart ohip-consumer@<chain>`. |
| 4406 | Bug no handshake (subprotocolo). Abrir incidente para o time. |
| 4409 | Automático (espera 2 min). Se persistir: procurar outro cliente usando a mesma app key (Postman, n8n, outra VM). |
| 4504 | Automático (15 s). Se persistir: status da Oracle. |

## Cuidados

- Nunca usar a app key de produção no Postman (ele não envia `ping` e derruba/disputa a conexão).
- Postman só com a coleção e o environment de `postman/` (sandbox, valores preenchidos só como *current value*, nunca commitados). Para explorar o Streaming no sandbox, use o `graphiql.html` oficial em `vendor/oracle-hospitality-api-docs/graphql/streaming/`.
- Atualizar as specs da Oracle só com `scripts/sync_oracle_api_docs.sh <commit>` e `make check` verde (ADR-0012).
- Replay só pela API/painel (`POST /api/v1/replay`); retenção do OHIP é de 7 dias.
- Expurgo: só pelo `ohip-purge` (seção acima), nunca `DELETE` manual fora dessa ordem.
- Alerta `ohip_merge_skipped_total` alto + offset menor que o salvo: o OHIP pode ter reiniciado os offsets (ambiente recriado, D-6). Parar o enricher, confirmar com a Oracle e decidir com o time de dados se as tabelas de domínio devem ser reconstruídas a partir do bruto.
- Alerta `falha_sistemica_no_lote` (consumer parado em `WAITING`, reconectando sem avançar): duas mensagens seguidas foram recusadas pelo banco sem nenhum progresso entre elas (ADR-0011). Primeiro conferir o Oracle (espaço em tablespace/undo, alert log, objetos inválidos) e corrigir; o consumer volta sozinho na próxima reconexão. Se o banco estiver saudável e o erro for de uma mensagem específica (ver o log e o último item de DLQ `CONSUME` da chain), `systemctl restart ohip-consumer@<chain>`: cada restart isola **uma** mensagem na DLQ. Depois da correção, reprocessar os itens pela API (`POST /api/v1/dlq/{id}/retry`).
- Alerta `oauth_recusado` (400/401/403 no token): credencial, app key, escopo ou `enterpriseId` errados, ou assinatura ainda não aprovada no Developer Portal. O processo não insiste em seguida; corrija a configuração e reinicie.
- Alerta `lease_perdido`: outro processo assumiu (failover) ou o banco ficou sem responder além de `LEASE_TTL_S`. O processo para de gravar e sai; o systemd o reinicia, e ele espera o lease. Duas instâncias ativas ao mesmo tempo nunca gravam juntas (barreira de epoch).
- `LeaseNotProvisionedError`: falta a linha em `OHIP_LEASE` (`sql/003_seed_leases.sql` com o `chain_code`).
- Redis fora: ingestão e publicação continuam; perdem-se o token compartilhado (cada processo pede o seu), o dedup rápido e as métricas de contadores. Testes contra um Redis de teste: `TEST_REDIS_URL` + `TEST_REDIS_DISPOSABLE=sim` e `make test-integration`.
- Units systemd com `RestartSec=10`: reiniciar em menos de 10 s após uma desconexão provoca 4409 e lockout de ~2 min.
