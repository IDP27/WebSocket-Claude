# CLAUDE.md — Consumidor de Eventos OHIP Streaming

Serviço Python que consome eventos do OPERA Cloud pela OHIP Streaming API (WebSocket + GraphQL), grava no Oracle 11g sem perda nem duplicidade e publica no RabbitMQ via outbox. O PRD vence em caso de conflito; na dúvida, pergunte.

Leia antes de qualquer tarefa: `docs/ARCHITECTURE.md`, `docs/PLAN.md`, `docs/OPEN_QUESTIONS.md`, `docs/OHIP_APIS.md` e os ADRs em `docs/adr/`. Contratos da Oracle: `vendor/oracle-hospitality-api-docs/` (ADR-0012), conferidos por `tests/contract/`.

## Regras de conduta

- **Não invente contratos da Oracle.** Onde a doc oficial não for conclusiva: deixe configurável, marque `# TODO(confirmar-doc)` e registre em `docs/OPEN_QUESTIONS.md`.
- Trabalhe **por fases** (`docs/PLAN.md`). Ao fim de cada uma: lint + testes, relatório do revisor e **pare para aprovação humana**.
- **Nunca** conecte ao OHIP de produção, rode DDL em banco real ou faça commit de segredos sem aprovação explícita.
- Dependência fora da stack abaixo exige ADR.
- Código, comentários e docs em **português**; identificadores em inglês.
- Contratos (fila, API, tabelas) só mudam com ADR (RNF-15).

## Stack obrigatória

Python 3.11+ · Poetry · systemd · FastAPI + Uvicorn · Flask + Gunicorn + Jinja2 (+HTMX) · Nginx · `websockets` (graphql-transport-ws manual) · `httpx` · `python-oracledb` **Thick** + Instant Client 19 · `aio-pika` · Redis · `pydantic-settings` · `structlog` · ruff, mypy, pytest, pytest-asyncio, respx, import-linter · py-spy.

## Oracle 11g — sempre

- Modo Thick; acesso **síncrono** (executor de 1 thread no consumer; rotas FastAPI `def`).
- Sem `IDENTITY`, `DEFAULT seq.NEXTVAL`, `FETCH FIRST`, JSON nativo ou particionamento. Use `SEQUENCE` explícita, `ROWNUM`, `CLOB`, `MERGE`.
- Identificadores ≤ 30 caracteres.

## Protocolo OHIP — não negociável (ADR-0007)

- `wss://<gateway>/subscriptions?key=<sha256 hex minúsculo da appKey>`, subprotocolo `graphql-transport-ws`.
- `connection_init` ≤ 5 s com `Authorization: Bearer` e `x-app-key`; `subscribe` com GUID; `ping` a cada 15 s; `next` conta como prova de vida.
- Encerrar: `complete` → drenar → **esperar o servidor fechar**; ≥ 10 s até o próximo `subscribe`; sempre enviar o último offset.
- 4401 novo token · 4403/4406 parar e alertar · 4409 esperar 120 s + jitter · 4504 esperar 15 s · rede: backoff exponencial com jitter.
- Um consumer por (appKey, chain, gateway): lease com prazo e epoch no Oracle; barreira `UPDATE ... WHERE epoch = :epoch` como **último comando** de toda transação de escrita (ADR-0008). Nada de trava no Redis.
- Regra dos 10 s vale também após crash/restart (`last_disconnect_at`, `RestartSec=10`).

## Garantias

- Evento + offset + outbox na **mesma transação**; offset só confirmado após commit. Nenhum evento é descartado antes do commit (fila cheia → espera; travou → conexão envenenada: nada mais dela é gravado, `complete` e reconecta pelo offset commitado).
- Erro por linha no lote: ORA-00001 nas constraints de dedup = duplicado; qualquer outro → DLQ `CONSUME` na mesma transação (ADR-0009).
- Dedup: `UNIQUE(unique_event_id)` e `UNIQUE(chain_code, offset_value, primary_key)` no Oracle; `ohip:seen` no Redis só **depois** do commit (ADR-0009).
- Publicação **só** via outbox (inclusive reprocessamento e retries); cabeça da fila por chain sem pular; uma mensagem por vez por chain esperando o confirm; falha de broker não conta tentativa (ADR-0002).
- Replay: troca do offset só com a conexão fechada, depois da drenagem (ARCHITECTURE §4.6).
- Offset é string `^[0-9]{1,20}$` (ADR-0006).

## Camadas (ADR-0005)

`entrypoints → application → domain`; adapters implementam ports. `domain`/`application` não importam `oracledb`, `websockets`, `fastapi`, `flask`, `aio_pika`, `redis`, `httpx`. O `ohip-admin` não importa adapters de Oracle/Redis (ADR-0004).

## Nunca

- Rodar o consumer dentro de Uvicorn/Gunicorn.
- `async def` em rota que toca o Oracle.
- Logar token, clientSecret, app key ou dados pessoais sem máscara.
- Criar tabelas de domínio ou regras de evento antes da resposta da Q-1.
- Otimizar sem medir; refatorar sem testes de caracterização.

## Papéis e comandos

- Subagentes em `.claude/agents/`: `arquiteto`, `engenheiro`, `revisor`, `otimizador`.
- Comandos em `.claude/commands/`: `/conceber`, `/entender`, `/depurar`, `/otimizar`, `/reestruturar`.
- Ciclo por fase: Arquiteto → Engenheiro → Revisor → (Otimizador) → aprovação humana. Achados críticos/altos voltam ao engenheiro antes do relatório.

## Definição de pronto

Testes unitários e de integração passando (≥ 80% no núcleo) · ruff, mypy e import-linter limpos · revisor sem achados críticos/altos · orçamentos de desempenho atendidos (quando aplicável) · ARCHITECTURE, RUNBOOK e ADRs atualizados · aprovação humana.

## Comandos de qualidade

```bash
make check              # tudo: ruff, mypy strict, import-linter, pytest + cobertura
make format             # ruff format + correções automáticas
make test-integration   # testes marcados oracle/rabbitmq/redis (precisa de infraestrutura)
```

Configuração: um grupo por prefixo em `src/ohip_streaming/config.py`, carregado com `load_settings(Grupo)` só nos entrypoints. Toda variável nova entra no `.env.example` (há teste). Logs: `get_logger()` e `bind_context(unique_event_id=..., chain_code=..., offset=...)` de `ohip_streaming.logging`.
