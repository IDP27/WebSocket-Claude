# Plano por fases

> Cada fase segue o ciclo Arquiteto → Engenheiro → Revisor → (Otimizador) → aprovação humana e só termina com a Definição de Pronto (CLAUDE.md).
> Estimativa relativa: **P** (pequena) · **M** (média) · **G** (grande).

| Fase | Entrega | Tamanho | Depende de | Bloqueios externos |
| --- | --- | --- | --- | --- |
| 0. Concepção | Docs, ADRs, DDL rascunho, estrutura, agentes e comandos | M | — | — |
| 1. Esqueleto ✅ aprovada | Poetry, `config.py` (pydantic-settings), logging JSON, ruff/mypy/pytest/import-linter, `.env.example`, `Makefile` | P | 0 | — |
| 2. Domain e application ✅ aprovada | `Event`, `Offset`, `ChainCode`, routing key, máscara LGPD, política de fechamento/backoff; ports; casos de uso com fakes | M | 1 | — |
| 3. Adapter Oracle ✅ aprovada (substituído pelo adapter PostgreSQL na Fase 11) | Pool Thick, `EventStore` (lote + epoch + dedup + outbox), repositórios de status/DLQ/replay; testes `@pytest.mark.oracle` rodando os mesmos cenários dos fakes, incluindo a classificação dos ORA de espaço (01653/01654/01688/01691/30036 → `StoreUnavailableError`, base do disjuntor, ADR-0011 §7) e a ordem atômica dos retries de DLQ | G | 2 | Oracle 11g de teste + Instant Client 19 |
| 4. Auth, lease e Redis (+ `LeaseSettings` comum, ADR-0010) ✅ aprovada | `TokenProvider` (OAuth, cache, margem), `Lease` no Oracle (ADR-0008), caches e métricas no Redis | M | 2, 3 | Q-10, Q-12, DV-12 |
| 5. Consumer WebSocket ✅ aprovada (validação no sandbox OHIP pendente) | Protocolo, heartbeat dinâmico, máquina de estados, códigos, replay, shutdown; `fake_ohip_server.py` e cenários | G | 3, 4 | Sandbox OHIP para validar D-1, D-4, D-7 (não bloqueia o código) |
| 6. Publisher outbox ✅ aprovada (validação em RabbitMQ real pendente) | Lotes por chain, publisher confirms, backoff com cabeça da fila, DLQ; testes de contrato | M | 3 | RabbitMQ de teste; Q-13 |
| 7. API FastAPI ✅ aprovada (planos de execução a conferir no Oracle real, Q-17) | Endpoints, perfis, auditoria, OpenAPI, `/metrics` | M | 3 | — |
| 8. Painel Flask ✅ aprovada (login no Nginx a definir, Q-5) | Páginas, componentes, HTMX, cliente da API | M | 7 | Q-5 |
| 9. Enricher ✅ aprovada (esqueleto; regras após a Q-1) | Esqueleto com cache, rate limit, `MERGE`; regras por evento após Q-1 | M (esqueleto) / G (regras) | 6 | Q-1, Q-2, assinatura das APIs REST |
| 10. Endurecimento ✅ aprovada (ADR-0020; carga medida com Oracle simulado, Oracle real na Q-17) | Teste de carga 10× (Otimizador), `/entender` geral, systemd, Nginx, expurgo, RUNBOOK, README; consumer gravando `subscription_id`, `last_message_at`, reconexões, `token_expires_at` e ping/pong em `OHIP_CONSUMER_STATUS` (pendência do status da API, ADR-0017) | G | 5–9 | Q-6, Q-7, Q-8 |
| 11. PostgreSQL ✅ aprovada (validação no banco da empresa pendente, Q-17/Q-18) | Troca do Oracle 11g pelo PostgreSQL (DV-15, ADR-0021/0022): adapter novo com os mesmos ports e cenários, DDL em `sql/postgres/`, remoção do Oracle | G | 3–10 | Q-18 (versão, hospedagem, DBA) |

## Refatorações do diagnóstico `/entender` (pós-Fase 10)

Uma por vez, cada uma com testes de caracterização escritos antes, revisão e aprovação humana (RNF-15). Nenhuma muda contrato de fila, API ou tabela.

| Passo | Entrega | Estado |
| --- | --- | --- |
| 1 | Espera interrompível e backoff únicos (`application/timing.py`, `domain/backoff.py`); corrige o `OverflowError` do backoff após ~1 025 falhas seguidas | ✅ aprovada |
| 2 | Contadores de alerta registrados com 0 na partida | ✅ na Fase 10 |
| 3 | Dividir `application/ports.py` em pacote por contexto (`application/ports/`, contratos `ports-por-contexto` e `monitoring-so-le-replay`) | ✅ aprovada |
| 4 | Extrair componentes de `_Session` (`consumer_session.py`: `StatusReporter`, `Liveness`, `ControlLoop`) | ✅ aprovada |
| 5 | Dividir `tests/fakes/memory.py` por port (`tests/fakes/memory/`: `MemoryState` + uma classe por port compondo o `InMemoryDatabase`) | ✅ aprovada |
| 6 | Composição comum dos processos assíncronos (`entrypoints/runtime.py`: `setup_logging`, `stop_on_signals`, `metrics_snapshots`) | ✅ aprovada |
| 7 | Separar o painel em identidade, erros e rotas (`admin/identity.py`, `errors.py`, `templating.py`, `views.py`, `context.py`) | ✅ aprovada |

### Diagnóstico `/entender` pós-Fase 11 (2026-10-21)

| Passo | Entrega | Estado |
| --- | --- | --- |
| 8 | B1 (`/depurar`): testes de índice com dados semeados e `ANALYZE` (passavam por acaso; falhavam num contêiner novo) | ✅ aprovada |
| 9 | B2 (`/depurar`): leituras de pendente do outbox por chain, sem varredura completa (`outbox_chains.py`; 12–15 ms → 0,5–0,7 ms com 400 mil linhas) | ✅ aprovada |
| 10 | Nome do lease numa fonte só (barreira do adapter = `LeaseKeeper` dos entrypoints) | proposto |
| 11 | Lista de máscara LGPD configurável (Q-9, ARCHITECTURE §11) | proposto |
| 12 | Stores sem wrappers duplicados (`_read`/`_write` comuns) | proposto |
| 13 | Limpezas: validador `module_codes` único, `UQ_EVT` morta, `shutdown()` dos executores, docs (`MERGE`/ORA) | proposto |
| 14 | *(opcional)* Extrair a política de reconexão do `ChainConsumer` | proposto |

## Preparação para as pendências externas (sem a Q-1)

Nada aqui cria regra de evento nem tabela de domínio (CLAUDE.md). Só deixa pronto o que destrava cada pendência quando ela chegar.

| Entrega | Destrava | Estado |
| --- | --- | --- |
| Pacote de decisão da Q-1 (`docs/pendencias/Q-1-escopo-de-eventos.md`): o que a doc oficial diz, perguntas a responder, passos da Fase 9b | Fase 9b (regras e tabelas de domínio) | ✅ aprovada |
| Pedido ao DBA da Aviva e checklist do ambiente (`docs/pendencias/Q-17-pedido-ao-dba.md`) | Validação das Fases 3 e 10 no banco real (refeito para PostgreSQL na Fase 11) | ✅ aprovada |
| Checklist único do sandbox (`docs/pendencias/sandbox-ohip.md`): D-1, D-3, D-4, D-7–D-11, D-13 e o ciclo de vida do consumer | Validação da Fase 5 | ✅ aprovada |
| Testes de integração que faltavam contra Redis e RabbitMQ reais (`tests/stores/test_redis_only.py`, `test_rabbitmq_only.py`): dedup e cache REST do enricher, snapshot de métricas lido pela API, cache de status, `REQUEUE`/descarte e fila divergente do enricher, `probe_broker` | Validação das Fases 6, 7 e 9 em broker e Redis reais. **Rodaram em 2026-10-06** contra Redis 7 e RabbitMQ 3 descartáveis (contêineres oficiais, Mac local): 12 de 12, incluindo os 6 testes das Fases 4 e 6 que nunca tinham rodado num broker real. Falta repetir no RabbitMQ e no Redis de teste da empresa (RUNBOOK, pré-requisito 7) | ✅ aprovada |

## Fase 11 — Troca do Oracle 11g pelo PostgreSQL (DV-15, ADR-0021, ADR-0022)

Decisão do tech lead em 2026-10-21. Padrões até a Q-18: PostgreSQL 15+ (testado em 16), acesso síncrono, saída total do Oracle, nada a migrar. Contratos de fila, API e tabelas **não mudam** (mesmas colunas e nomes; só os tipos). Estado: **✅ aprovada** (Arquiteto `9334f13`, Engenheiro `522366d`). Falta repetir os testes no PostgreSQL da empresa (Q-17/Q-18).

Tarefas do Engenheiro, nesta ordem, cada uma com `make check` verde:

| # | Tarefa | Pronto quando |
| --- | --- | --- |
| 11.1 | Dependências: `psycopg[binary,pool]>=3.2` no `pyproject.toml`; `PostgresSettings` (grupo `POSTGRES_`, ADR-0021 §7), `API_DB_STATEMENT_TIMEOUT_MS`; `.env.example` | `test_config` cobre as variáveis novas e o `.env.example`  ✅ |
| 11.2 | Imagem `postgres:16` (download com permissão do usuário); alvos `make test-db` / `make test-db-down` (ADR-0021 §9: o alvo cria o próprio contêiner, confere banco vazio, `ON_ERROR_STOP` + `--single-transaction`); `EXPLAIN` de `RECENT_EVENTS_SQL` e do `DELETE` do expurgo, com `SET enable_seqscan = off` (o volume de teste é pequeno; com volume realista, Q-18, sem o ajuste) | Schema criado só no contêiner do alvo; os dois `EXPLAIN` usam os índices de data  ✅ |
| 11.3 | `adapters/postgres/database.py`, `errors.py` e `rows.py`: sessões (dedicada, pool, inline) com `autocommit=True`, transação explícita, barreira, classificação por SQLSTATE, troca de NUL e surrogates, fuso (`astimezone`); inventário de `''` × `NULL` (ADR-0021 §4); driver falso roteirizado em `tests/fakes/` | Unitários de sessão (nenhuma leitura deixa transação aberta) e de classificação, inclusive `53100`, `57014`, `25006`, `25P03`, `57P05` e `55P03`  ✅ |
| 11.4 | `event_store` com os dois caminhos do ADR-0022. Linha que não codifica em UTF-8 (surrogate) vai direto para o caminho lento, por checagem antes do `executemany`, sem depender do estado do pipeline após a exceção | Os 5 testes obrigatórios do ADR-0022 §6  ✅ |
| 11.5 | Demais stores: outbox, lease, status, replay, operations, monitoring, enrichment (`ON CONFLICT`), purge | Cenários de `tests/stores/` verdes no fake e no PostgreSQL (`-m postgres`)  ✅ |
| 11.6 | Entrypoints e API passam a compor o adapter PostgreSQL; `.importlinter` com `psycopg` proibido em `domain`/`application`/admin | Todos os processos sobem contra o contêiner; contratos de camadas mantidos  ✅ |
| 11.7 | Teste de carga com `LOAD_DB_*` medido no contêiner, mais o cenário de 1 linha ruim a cada 1.000 (ADR-0022 §3) | p95 ≤ 5 s a 10× o pico; comparação com o resultado da Fase 10  ✅ |
| 11.8 | Remoção do Oracle: `adapters/oracle/`, `sql/001`–`003`/`900`, `oracledb`, marcador `oracle`, Instant Client no RUNBOOK e nas units, tarefas do VS Code | Nenhuma referência ao driver Oracle fora do histórico; `make check` verde  ✅ |
| 11.9 | Documentos: ARCHITECTURE §5 e §9, RUNBOOK, README, `docs/pendencias/Q-17-pedido-ao-dba.md` refeito para PostgreSQL | Revisor sem achados críticos/altos; aprovação humana  ✅ |

## Caminho crítico

```
0 → 1 → 2 ── 3 ── 4 ── 5 ──────────┐
                                   ├─ 10
                 3 ─ 6 ─ 9 ────────┤
                 3 ─ 7 ─ 8 ────────┘
```

Fases 6 e 7 podem andar em paralelo com a 4 e a 5 depois da 3. (A Fase 4 passou a depender da 3 porque o lease fica no Oracle.)

## Ações externas para iniciar já (semana 1)

1. Pedir habilitação de streaming no ambiente UAT (workshop B93152 para clientes, ou SR) e registrar a app com os eventos da fase 1. **Processo manual da Oracle, risco alto de prazo.**
2. Criar uma app key **separada para desenvolvimento** (a doc proíbe dividir a mesma app entre desenvolvedores).
3. Providenciar PostgreSQL de teste com a versão de produção (Q-17/Q-18) e liberação de saída `wss://` 443 para o gateway (sem inspeção TLS).
4. Responder Q-1, Q-2, Q-4 e Q-8.

## Três maiores riscos técnicos

| # | Risco | Impacto | Mitigação no desenho |
| --- | --- | --- | --- |
| 1 | **Pré-requisitos manuais da Oracle** (habilitação de streaming via workshop B93152/SR, aprovação dos eventos pelo dono do ambiente) atrasam o acesso ao sandbox/UAT | Alto: sem eles não há validação real de D-1, D-4, D-7, D-9 | Pedir na semana 1; desenvolver contra o servidor simulado, que reproduz o protocolo do guia; validações de sandbox concentradas num checklist único |
| 2 | **Consumidor único e ordem**: split-brain entre instâncias, reconexão em menos de 10 s após crash (4409 com lockout de 2 min) e falhas parciais de lote | Alto: perda, duplicidade ou parada da chain | Lease + epoch no Oracle (ADR-0008), `last_disconnect_at` persistido e `RestartSec=10` (ADR-0007), `batcherrors` → duplicado ou DLQ, bisseção do lote (ADR-0009) |
| 3 | **Vazão com o banco síncrono** em rajadas (backpressure > 1,8 MB, fechamento do dia), com `pong` adiado e fila interna enchendo | Médio/alto: atraso acima do p95 de 5 s ou reconexões | Micro-lotes com `executemany`, fila limitada que nunca descarta, heartbeat dinâmico (`max(180 s, 4×SRTT)`), teste de carga a 10× na Fase 10; plano B: avaliar 19c + modo Thin/async (só troca o adapter) |

Outros riscos do PRD seguem válidos (LGPD, mudança de contrato da Oracle, VM única, n8n como consumidor WebSocket).
