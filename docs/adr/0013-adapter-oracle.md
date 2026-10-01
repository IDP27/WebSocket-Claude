# ADR-0013 — Adapter Oracle (Fase 3)

- Status: Proposto (Fase 3)
- Data: 2026-10-01

## Contexto

A Fase 3 implementa os ports da Fase 2 no Oracle 11g em modo Thick (ADR-0003), seguindo o contrato transacional escrito nos ports e conferido pelos fakes (ADR-0011). Não há Oracle nem Instant Client 19 na máquina de desenvolvimento: o Mac é arm64, e o Instant Client 19 não existe para essa arquitetura. A validação contra banco real fica para quando houver um schema de teste.

## Decisão

1. **Sessões** (`adapters/oracle/database.py`):
   - `DedicatedSession`: uma conexão fixa e um thread, para a escrita do consumer (ordem das transações da chain).
   - `PooledSession`: conexão do pool por chamada, para publisher, controle e API. Tem uma versão síncrona `call()` para as rotas `def` do FastAPI.
   - Indisponibilidade descarta a conexão (`pool.drop`); a próxima chamada pega outra.
2. **Pool**: `init_oracle_client` uma vez por processo; `POOL_GETMODE_TIMEDWAIT` com `ORACLE_POOL_WAIT_TIMEOUT_MS` (padrão 10 s); `call_timeout` por conexão com `ORACLE_CALL_TIMEOUT_MS` (padrão 60 s, 0 desliga). Os dois estouros contam como banco indisponível.
3. **Classificação de erros** (`errors.py`):
   - indisponível: rede, sessão, instância, tempo de chamada, transação que precisa ser desfeita (25402), `isrecoverable`, erros `DPI-`/`DPY-` de conexão e **falta de recurso**: espaço (01536, 01628, 01631, 01632, 01650, 01651, 01652, 01653, 01654, 01658, 01659, 01683, 01688, 01691, 01692, 30036), archiver (00257), memória (04030, 04031) e cursores esgotados (01000);
   - ORA-00001 em `OHIP_EVENT_RAW_UQ_EVT`/`UQ_OFF` = duplicado; na PK ou em outra constraint, falha da linha;
   - o resto vira `BatchFailedError` no lote do consumer e `StoreOperationError` nas demais operações.
   - **Erro de recurso por linha** (`getbatcherrors`) desfaz o lote inteiro como indisponível: tablespace cheio nunca vira DLQ.
4. **Transação**: corpo → commit; **qualquer** exceção, inclusive um bug do lado Python no meio do corpo, → rollback (falha no rollback só é logada); erros do driver viram erro da aplicação. A `DedicatedSession` descarta a conexão em indisponibilidade e em exceção inesperada; com erro da aplicação (rollback feito), mantém. A composição falha cedo se faltar um exchange (`ExchangeKind`), nunca no meio do lote. A barreira de epoch é o último comando. **Commit que falha por queda tem resultado desconhecido**: sobe como indisponível, o consumer reconecta pelo offset confirmado e a deduplicação absorve o que tiver sido gravado.
5. **Lote do consumer**: `executemany(batcherrors=True)` no evento bruto; outbox só dos gravados, na ordem de chegada (`NEXTVAL` na ordem do array); DLQ; item de retry; offset (`rowcount` 0 = chain não provisionada → `UnknownChainError`); barreira.
6. **Tipos e textos**:
   - `setinputsizes` fixa CLOB e TIMESTAMP, para um `None` na primeira linha não decidir o tipo da coluna. Cada `executemany` usa um cursor próprio da mesma conexão (mesma transação), para os tipos não vazarem para o comando seguinte.
   - CLOB é lido como `str` por `outputtypehandler`.
   - `TIMESTAMP` guarda UTC sem fuso; a aplicação só trabalha com `datetime` UTC com fuso (sem fuso → erro).
   - Os horários de auditoria vêm do banco (`SYS_EXTRACT_UTC(SYSTIMESTAMP)`). `next_attempt_at` vem do relógio do publisher, que é quem o compara.
   - `VARCHAR2(n)` conta bytes: mensagens de erro são cortadas em UTF-8 sem quebrar caractere.
7. **Retries de DLQ**: `UPDATE` condicional (`resolution IS NULL`) primeiro e, com `rowcount` 1, o `INSERT`. Como o 11g não aceita `RETURNING` em `INSERT ... SELECT`, a cópia da outbox pega o id da sequence antes.
8. **Status do consumer**: novo port mínimo `ConsumerStatusStore` (`record_state`, `record_disconnect`, `disconnect_snapshot`), só com o que a regra dos 10 s precisa, com horário do banco. Heartbeat e RTT entram na Fase 5. Status não passa pela barreira de epoch (é informativo).
9. **DDL (rascunho, nunca aplicado)**: `OHIP_REPLAY_REQUEST` ganha `cancelled_at` e `cancelled_by`, porque o port de cancelamento recebe quem cancelou e não havia onde guardar.
10. **Fora desta fase**: `EnrichmentStore` (depende das tabelas de domínio, Q-1 → Fase 9) e lease (aquisição e renovação → Fase 4). A barreira de epoch já está pronta.
11. **Testes**:
    - driver falso roteirizado (`tests/fakes/oracle_driver.py`) para a ordem dos comandos, os rollbacks e a classificação;
    - **os mesmos cenários de contrato** (`tests/stores/`) rodando no fake em memória e no Oracle;
    - testes só de Oracle real: erros de espaço gerados por PL/SQL com `PRAGMA EXCEPTION_INIT` (sem DDL) e corrida real de dois retries de DLQ disputando o lock da linha.
    - O Oracle de teste exige `TEST_ORACLE_*` mais `TEST_ORACLE_DISPOSABLE_SCHEMA=sim`. Os testes não rodam DDL; só DML nas chains `ZZT*`. Antes de qualquer DML, recusam o schema se houver chain fora de `ZZT*` em `OHIP_OFFSET`/`OHIP_OUTBOX` ou lease `publisher` ativo com outro dono. Ao pegar o lease, o teste deixa `owner = 'zzt-test'` com prazo vencido.
    - Corridas reais cobertas: dois retries de DLQ disputando o item; zumbi parado na barreira enquanto o novo dono incrementa o epoch (ADR-0008).
    - **Limitação**: um erro de espaço **por linha** (`getbatcherrors`) só é provado com o driver falso. No Oracle real, provocá-lo exigiria encher um tablespace (DDL). O caminho de chamada inteira é provado com `EXCEPTION_INIT`.

## Consequências

- O adapter fica coberto por testes unitários (cobertura acima de 99%), mas a prova contra o Oracle 11g real **está pendente**: precisa de um schema de teste com os scripts `sql/` aplicados pelo DBA e de uma VM x86 com Instant Client 19 (RUNBOOK).
- A API (Fase 7) pode chamar `PooledSession.call` diretamente nas rotas `def`.
- Novas variáveis: `ORACLE_POOL_WAIT_TIMEOUT_MS` e `ORACLE_CALL_TIMEOUT_MS` (`.env.example`).
