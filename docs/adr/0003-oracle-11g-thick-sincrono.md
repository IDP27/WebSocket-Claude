# ADR-0003 — Oracle 11g com python-oracledb em modo Thick, acesso síncrono

- Status: Aceito (aprovado em 2026-10-01 com a Fase 0)
- Data: 2026-10-01

## Contexto

O banco confirmado é Oracle 11g. O modo Thin do `python-oracledb` só conecta a 12.1+, e a API asyncio do driver só existe no modo Thin. O 11g não tem `IDENTITY`, `FETCH FIRST`, tipo JSON nem `DEFAULT seq.NEXTVAL`, e o particionamento exige licença adicional.

## Decisão

- `oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_LIB_DIR)` com Oracle Instant Client 19 na VM.
- Acesso síncrono:
  - **consumer**: um `ThreadPoolExecutor(max_workers=1)` com uma conexão dedicada para as escritas (um único thread preserva a ordem das transações da chain) e uma segunda conexão de controle, em outro thread, para lease, status e pedidos de replay (ADR-0008).
  - **publisher/enricher**: executor pequeno com pool de conexões; chamadas via `ohip_streaming.logging.run_in_executor`, que leva o contexto de correlação para a thread (`loop.run_in_executor` puro o perderia — ADR-0010). O mesmo vale para o executor do consumer.
  - **API**: rotas que tocam o Oracle são `def` (threadpool do FastAPI), pool de sessão do driver.
- IDs por sequence explícita no `INSERT`; lotes reservam ids com `CONNECT BY LEVEL`.
- Paginação por chave com `ROWNUM`; JSON em `CLOB` com `setinputsizes`.
- Inserts em lote com `executemany(..., batcherrors=True)` para tratar erros por linha (duplicados e DLQ — ADR-0009).
- Expurgo por data com `DELETE` em lotes; sem particionamento.
- Adapter Oracle isolado (ADR-0005): migrar para 19c+ e modo Thin/async afeta só esse adapter.

## Consequências

- A VM precisa do Instant Client 19 (dependência de infraestrutura no RUNBOOK).
- Uma transação lenta no consumer atrasa a chain inteira; mitigado por micro-lotes e fila interna limitada. Com a fila cheia, o laço de leitura **espera** (nunca descarta); o envio de `ping` continua; se a espera passar de `INTAKE_STALL_TIMEOUT`, o consumer faz `complete` e reconecta pelo último offset commitado (ARCHITECTURE §4.1).
- O 11g está fora de suporte padrão (risco no PRD).
