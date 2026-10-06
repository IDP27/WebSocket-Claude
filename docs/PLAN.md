# Plano por fases

> Cada fase segue o ciclo Arquiteto → Engenheiro → Revisor → (Otimizador) → aprovação humana e só termina com a Definição de Pronto (CLAUDE.md).
> Estimativa relativa: **P** (pequena) · **M** (média) · **G** (grande).

| Fase | Entrega | Tamanho | Depende de | Bloqueios externos |
| --- | --- | --- | --- | --- |
| 0. Concepção | Docs, ADRs, DDL rascunho, estrutura, agentes e comandos | M | — | — |
| 1. Esqueleto ✅ aprovada | Poetry, `config.py` (pydantic-settings), logging JSON, ruff/mypy/pytest/import-linter, `.env.example`, `Makefile` | P | 0 | — |
| 2. Domain e application ✅ aprovada | `Event`, `Offset`, `ChainCode`, routing key, máscara LGPD, política de fechamento/backoff; ports; casos de uso com fakes | M | 1 | — |
| 3. Adapter Oracle ✅ aprovada (validação em Oracle real pendente, Q-17) | Pool Thick, `EventStore` (lote + epoch + dedup + outbox), repositórios de status/DLQ/replay; testes `@pytest.mark.oracle` rodando os mesmos cenários dos fakes, incluindo a classificação dos ORA de espaço (01653/01654/01688/01691/30036 → `StoreUnavailableError`, base do disjuntor, ADR-0011 §7) e a ordem atômica dos retries de DLQ | G | 2 | Oracle 11g de teste + Instant Client 19 |
| 4. Auth, lease e Redis (+ `LeaseSettings` comum, ADR-0010) ✅ aprovada | `TokenProvider` (OAuth, cache, margem), `Lease` no Oracle (ADR-0008), caches e métricas no Redis | M | 2, 3 | Q-10, Q-12, DV-12 |
| 5. Consumer WebSocket ✅ aprovada (validação no sandbox OHIP pendente) | Protocolo, heartbeat dinâmico, máquina de estados, códigos, replay, shutdown; `fake_ohip_server.py` e cenários | G | 3, 4 | Sandbox OHIP para validar D-1, D-4, D-7 (não bloqueia o código) |
| 6. Publisher outbox ✅ aprovada (validação em RabbitMQ real pendente) | Lotes por chain, publisher confirms, backoff com cabeça da fila, DLQ; testes de contrato | M | 3 | RabbitMQ de teste; Q-13 |
| 7. API FastAPI ✅ aprovada (planos de execução a conferir no Oracle real, Q-17) | Endpoints, perfis, auditoria, OpenAPI, `/metrics` | M | 3 | — |
| 8. Painel Flask ✅ aprovada (login no Nginx a definir, Q-5) | Páginas, componentes, HTMX, cliente da API | M | 7 | Q-5 |
| 9. Enricher ✅ aprovada (esqueleto; regras após a Q-1) | Esqueleto com cache, rate limit, `MERGE`; regras por evento após Q-1 | M (esqueleto) / G (regras) | 6 | Q-1, Q-2, assinatura das APIs REST |
| 10. Endurecimento ✅ aprovada (ADR-0020; carga medida com Oracle simulado, Oracle real na Q-17) | Teste de carga 10× (Otimizador), `/entender` geral, systemd, Nginx, expurgo, RUNBOOK, README; consumer gravando `subscription_id`, `last_message_at`, reconexões, `token_expires_at` e ping/pong em `OHIP_CONSUMER_STATUS` (pendência do status da API, ADR-0017) | G | 5–9 | Q-6, Q-7, Q-8 |

## Refatorações do diagnóstico `/entender` (pós-Fase 10)

Uma por vez, cada uma com testes de caracterização escritos antes, revisão e aprovação humana (RNF-15). Nenhuma muda contrato de fila, API ou tabela.

| Passo | Entrega | Estado |
| --- | --- | --- |
| 1 | Espera interrompível e backoff únicos (`application/timing.py`, `domain/backoff.py`); corrige o `OverflowError` do backoff após ~1 025 falhas seguidas | ✅ aprovada |
| 2 | Contadores de alerta registrados com 0 na partida | ✅ na Fase 10 |
| 3 | Dividir `application/ports.py` em pacote por contexto (`application/ports/`, contratos `ports-por-contexto` e `monitoring-so-le-replay`) | ✅ aprovada |
| 4 | Extrair componentes de `_Session` (`consumer_session.py`: `StatusReporter`, `Liveness`, `ControlLoop`) | ✅ aprovada |
| 5 | Dividir `tests/fakes/memory.py` por port (`tests/fakes/memory/`: `MemoryState` + uma classe por port compondo o `InMemoryDatabase`) | ✅ aprovada |
| 6 | Composição comum dos processos assíncronos (`entrypoints/runtime.py`) | pendente |
| 7 | Separar o painel em identidade, erros e rotas | pendente |

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
3. Providenciar schema Oracle 11g de teste, Instant Client 19 na VM e liberação de saída `wss://` 443 para o gateway (sem inspeção TLS).
4. Responder Q-1, Q-2, Q-4 e Q-8.

## Três maiores riscos técnicos

| # | Risco | Impacto | Mitigação no desenho |
| --- | --- | --- | --- |
| 1 | **Pré-requisitos manuais da Oracle** (habilitação de streaming via workshop B93152/SR, aprovação dos eventos pelo dono do ambiente) atrasam o acesso ao sandbox/UAT | Alto: sem eles não há validação real de D-1, D-4, D-7, D-9 | Pedir na semana 1; desenvolver contra o servidor simulado, que reproduz o protocolo do guia; validações de sandbox concentradas num checklist único |
| 2 | **Consumidor único e ordem**: split-brain entre instâncias, reconexão em menos de 10 s após crash (4409 com lockout de 2 min) e falhas parciais de lote | Alto: perda, duplicidade ou parada da chain | Lease + epoch no Oracle (ADR-0008), `last_disconnect_at` persistido e `RestartSec=10` (ADR-0007), `batcherrors` → duplicado ou DLQ, bisseção do lote (ADR-0009) |
| 3 | **Vazão com Oracle 11g síncrono** em rajadas (backpressure > 1,8 MB, fechamento do dia), com `pong` adiado e fila interna enchendo | Médio/alto: atraso acima do p95 de 5 s ou reconexões | Micro-lotes com `executemany`, fila limitada que nunca descarta, heartbeat dinâmico (`max(180 s, 4×SRTT)`), teste de carga a 10× na Fase 10; plano B: avaliar 19c + modo Thin/async (só troca o adapter) |

Outros riscos do PRD seguem válidos (LGPD, mudança de contrato da Oracle, VM única, n8n como consumidor WebSocket).
