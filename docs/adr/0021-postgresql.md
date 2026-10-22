# ADR-0021 — Troca do Oracle 11g pelo PostgreSQL

- Status: Proposto (Fase 11)
- Data: 2026-10-21
- Substitui: ADR-0003 (Oracle 11g Thick síncrono) e ADR-0013 (adapter Oracle). Altera o ADR-0009 (gravação do lote, detalhada no ADR-0022), o ADR-0008 (só a sintaxe da barreira) e o ADR-0019 (`MERGE` → `INSERT ... ON CONFLICT`).

## Contexto

O PRD e o CLAUDE.md fixam o Oracle 11g (RNF-02, Q-3). Em 2026-10-21 o tech lead do time decidiu usar **PostgreSQL** e o usuário aprovou iniciar a mudança. É uma divergência deliberada do PRD, registrada como **DV-15** em `docs/OPEN_QUESTIONS.md`.

Nada foi para produção e não há dados a migrar. O Oracle está isolado em `adapters/oracle/` (cerca de 2.400 linhas) e em `sql/` (ADR-0005). `domain`/`application` não importam o driver: o import-linter confere. Os ports e os cenários de contrato de `tests/stores/` independem do banco e servem de critério de aceitação do adapter novo.

Padrões assumidos até a confirmação do usuário (Q-18):
- PostgreSQL **15 ou superior**, testado em contêiner `postgres:16`;
- acesso **síncrono**, como hoje;
- saída **total** do Oracle;
- nenhum dado a migrar.

## Decisão

### 1. Driver e dependências

- `psycopg` 3 com `psycopg_pool`, pacote `psycopg[binary,pool]>=3.2`. A versão mínima é exigência do ADR-0022: `executemany(..., returning=True)` (3.1+) e `cursor.results()` (3.2+). A roda binária traz a libpq: a VM não precisa de cliente instalado.
- Sai `oracledb` e, com ele, o Instant Client 19.
- Dependência nova da stack: o CLAUDE.md passa a listá-la.

### 2. Acesso síncrono, mesmo desenho de sessões

Os ports continuam `async`, e o adapter roda o driver síncrono em executor, como o ADR-0003 definiu:

| Sessão | Hoje (Oracle) | PostgreSQL |
| --- | --- | --- |
| `DedicatedSession` | Uma conexão fixa e um thread; escrita do consumer | Igual, com `psycopg.Connection` |
| `PooledSession` | Conexão do pool por chamada; publisher, enricher, controle do consumer, expurgo | `psycopg_pool.ConnectionPool` |
| `InlineSession` | Pool no thread de quem chama; rotas `def` da API (ADR-0017) | Igual |

- Mantêm-se o executor de 1 thread no consumer, as rotas `def` na API e o `run_in_executor` que leva o contexto de log.
- O modo assíncrono do psycopg fica como evolução futura, com ADR próprio e teste de carga.

### 3. Conexão e prazos

- **Transações explícitas (achado da revisão):** toda conexão é aberta com `autocommit=True`.
  - Escrita sempre dentro de `with conn.transaction():`. Blocos aninhados viram savepoints, que é o mecanismo do ADR-0022.
  - Leitura (`query`) roda fora de transação: nada fica "idle in transaction". No psycopg sem autocommit, todo `SELECT` abriria uma transação que ficaria aberta até o próximo commit. A conexão de escrita do consumer, que lê `pending_consume_retries` a cada passo do controle, ficaria parada em transação indefinidamente: segurando lock de leitura em `ohip_dlq`, congelando o `now()` do lote seguinte e sujeita a `idle_in_transaction_session_timeout`.
  - Teste no driver falso: depois de `query()`, a conexão não está em transação.
- Toda conexão abre com `TimeZone=UTC`, `application_name=ohip-<processo>` e `statement_timeout` configurável.
- **Prazos:**
  - `statement_timeout` substitui o `call_timeout`;
  - o `timeout` do pool substitui o `wait_timeout`;
  - estouro dos dois = banco indisponível, como hoje.
- Isolamento `READ COMMITTED` (padrão do PostgreSQL, igual ao Oracle). A barreira de epoch (ADR-0008) funciona igual: um `UPDATE ... WHERE epoch = %s` que concorre com quem incrementou o epoch reavalia a condição na versão nova da linha e afeta 0 linhas.
- **TLS** (RNF-07): `POSTGRES_SSLMODE`, padrão `verify-full`. Fora de `APP_ENVIRONMENT=desenvolvimento`, só `verify-full` ou `verify-ca` são aceitos: `allow`/`prefer` caem para texto puro sem avisar e `require` não confere o certificado (achado da revisão do Engenheiro).

### 4. Tradução de SQL

| Oracle 11g | PostgreSQL |
| --- | --- |
| `SYS_EXTRACT_UTC(SYSTIMESTAMP)` em comando e em `WHERE` | `statement_timestamp()`: horário de cada comando, constante dentro dele, como o `SYSTIMESTAMP`. É STABLE, então um filtro `received_at >= statement_timestamp() - ...` usa índice. **Nunca `clock_timestamp()`** (VOLATILE: impede o uso de índice no `/status` e no expurgo; achado da revisão) |
| `DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP)` | `DEFAULT now()` |
| `TIMESTAMP` (UTC sem fuso) | `timestamptz`; o driver devolve `datetime` com fuso, sem conversão manual |
| `NUMTODSINTERVAL(:s, 'SECOND')` / `NUMTODSINTERVAL(:d, 'DAY')` | `make_interval(secs => %(s)s)` / `make_interval(days => %(d)s)` |
| `ROWNUM <= :n` | `LIMIT %(n)s` |
| `DELETE ... AND ROWNUM <= :n` | `DELETE ... WHERE id IN (SELECT id ... LIMIT %(n)s)` |
| `seq.NEXTVAL FROM dual CONNECT BY LEVEL <= :n` | `SELECT nextval('seq') FROM generate_series(1, %(n)s)` |
| `RETURNING x INTO :var` | `RETURNING x` (lido pelo `fetchone`) |
| `SELECT ... FROM dual` (`PING_SQL`, `DB_NOW_SQL`, reserva de ids) | `SELECT ...` sem `FROM` |
| Binds `:nome` | `%(nome)s`. Com parâmetros, todo `%` literal vira `%%` (ex.: `LIKE 'ZZT%%'` em `tests/stores/backends.py`) |
| `INSERT ... SELECT` com `seq.NEXTVAL` (DLQ `PUBLISH`, cópia da outbox) | `INSERT ... SELECT nextval(...) ... RETURNING id` (o 11g não aceitava `RETURNING` aqui; o PostgreSQL aceita) |
| `to_db` remove o fuso; `from_db` faz `.replace(tzinfo=UTC)` (`rows.py`) | Escrita com o `datetime` UTC com fuso; leitura com `.astimezone(UTC)`. `rows.py` é reescrito |
| `MERGE` condicional (enricher) | `INSERT ... ON CONFLICT (chave) DO UPDATE ... WHERE` (atômico com N enrichers em paralelo) |
| `CLOB` | `text` |
| `VARCHAR2(n)` em bytes | `varchar(n)` em caracteres. O corte de texto por bytes continua valendo, porque n bytes cabem em n caracteres |
| `NUMBER(19)` / `NUMBER(5)` | `bigint` / `integer` |
| `''` é `NULL` (recusado em `NOT NULL`, lido como `None`) | `''` é texto vazio, diferente de `NULL` |
| Índice único por função (`CASE WHEN status = 'PENDING'`) | Índice único parcial (`WHERE status = 'PENDING'`) |
| `PERCENTILE_CONT` em intervalo | `percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ...))` |
| Identificador ≤ 30 caracteres | Sem esse limite (até 63). Os nomes atuais ficam iguais |

- **Duas diferenças de semântica (achado da revisão):**
  - **Tamanho em caracteres.** O domínio valida tamanhos em caracteres (`domain/events.py`). No Oracle, um campo com acento podia passar no domínio e ser recusado por bytes (ORA-12899 → DLQ). No PostgreSQL ele é gravado. É mais permissivo e inofensivo. O conjunto de DLQ pode diferir do Oracle só nesses casos.
  - **`''` × `NULL`.** O Oracle tratava `''` como `NULL`. Os campos do evento já são normalizados pelo domínio, mas textos livres (`error_message`, `reason`, `instance_id`, motivos) não. A tarefa 11.3 inventaria onde o código conta com `''` = `NULL` (ex.: `raw or ""` em `event_store.py`) e decide caso a caso. A regra padrão é o adapter gravar `NULL` no lugar de `''` em colunas opcionais.
- **Payload como `text`, não `jsonb`.** O `jsonb` reordena chaves e descarta duplicadas: o payload deixaria de ser "como recebido" (RF-04, RF-13). A tabela de domínio (Q-1) pode usar `jsonb` se o ADR dela pedir.
- **Sequências explícitas, mantidas.** O port `reserve_event_ids` reserva os ids antes do `INSERT`, para a outbox referenciar o evento no mesmo lote.
  - `ohip_outbox_seq` usa **`CACHE 1`**. No PostgreSQL o cache de sequência é **por sessão**: com cache maior, a sessão da API (reprocessamento) e a do consumer receberiam blocos intercalados. A ordem dos ids da outbox deixaria de seguir a ordem de gravação, que é a base da cabeça da fila por chain (ADR-0002).
  - A sequência dos eventos brutos pode ter cache, porque o id dela não define ordem de publicação.

### 5. Classificação de erros por SQLSTATE

Substitui a lista de `ORA-` (ADR-0013 §3) em `adapters/postgres/errors.py`:

| SQLSTATE | Significado | Tratado como |
| --- | --- | --- |
| classe `08` | conexão | indisponível |
| classe `53` (`53100` disco, `53200` memória, `53300` conexões) | falta de recurso | indisponível (base do disjuntor; nunca vira DLQ) |
| `57P01`, `57P02`, `57P03` | servidor desligando, crash, ainda subindo | indisponível |
| `57014` | `statement_timeout` estourado | indisponível (equivale ao `call_timeout`) |
| `40001`, `40P01` | serialização, deadlock | indisponível (transiente: tenta de novo) |
| `25006` | transação somente leitura (conexão caiu num standby depois de failover; equivale ao ORA-25402) | indisponível |
| `25P03`, `57P05` | sessão encerrada por ociosidade em transação / ociosa (equivalem ao ORA-02396) | indisponível |
| `55P03` | lock não obtido (`lock_timeout`) | indisponível |
| classe `58` | erro de E/S do servidor | indisponível |
| sem SQLSTATE (`OperationalError`, `PoolTimeout`) | conexão perdida, pool esgotado | indisponível |
| `23505` em `ohip_event_raw_uq_evt` / `_uq_off` | duplicado | duplicado (ADR-0009) |
| classes `22` e `23` (outros) | dado ou constraint | falha da linha (DLQ) ou `StoreOperationError` |
| o resto | — | `BatchFailedError` no lote, `StoreOperationError` nas demais |

- O nome da constraint vem de `error.diag.constraint_name`, sem interpretar texto de mensagem.
- O PostgreSQL devolve os nomes **em minúsculas** (`ohip_event_raw_uq_evt`). As constantes `DEDUP_CONSTRAINTS` e `PENDING_INDEX`, hoje em maiúsculas, mudam junto.
- `23505` em `ohip_replay_ux_pending` vira `ReplayAlreadyPendingError` (409 da API), como hoje.

### 6. Texto com NUL

O PostgreSQL recusa o caractere `\x00` em `text`/`varchar` (`22021`). Uma mensagem bruta inválida com NUL iria para a DLQ e derrubaria a gravação da própria DLQ, levando o lote inteiro. Por isso:
- todo texto livre gravado pelo adapter (`raw_message`, `error_message`, `stack_trace`, `last_error`, motivos) troca `\x00` por `U+FFFD`;
- o payload do evento não é alterado. Com NUL, ele falha como linha e vai para a DLQ (ADR-0022), onde é gravado com a troca. **Confirmado na implementação:** quem recusa o NUL é o próprio psycopg, antes de enviar (`DataError` sem SQLSTATE), e não o servidor (`22021`); o adapter trata os dois como erro de dado da linha.
- **Surrogate isolado** (ex.: `"\ud800"` vindo do JSON) falha na codificação **do lado do cliente** (`UnicodeEncodeError`, sem SQLSTATE):
  - no texto livre, recebe a mesma troca por `U+FFFD`;
  - num campo do evento, o adapter trata `UnicodeEncodeError` como erro de dado da linha (ADR-0022 §1): caminho lento e DLQ, nunca envenenamento da conexão;
  - um teste confirma o caminho.

### 7. Configuração

Grupo `POSTGRES_` no lugar de `ORACLE_`:

| Variável | Padrão | Antes |
| --- | --- | --- |
| `POSTGRES_HOST`, `POSTGRES_PORT` (5432), `POSTGRES_DB` | — | `ORACLE_DSN` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | — | `ORACLE_USER` / `ORACLE_PASSWORD` |
| `POSTGRES_SSLMODE` | `verify-full` | — |
| `POSTGRES_SSLROOTCERT` | vazio (certificados do sistema) | — |
| `POSTGRES_POOL_MIN` / `POSTGRES_POOL_MAX` | 1 / 4 | `ORACLE_POOL_*` |
| `POSTGRES_POOL_WAIT_TIMEOUT_MS` | 10000 | `ORACLE_POOL_WAIT_TIMEOUT_MS` |
| `POSTGRES_STATEMENT_TIMEOUT_MS` | 60000 (0 desliga) | `ORACLE_CALL_TIMEOUT_MS` |
| `API_DB_STATEMENT_TIMEOUT_MS` | 15000 | `API_ORACLE_CALL_TIMEOUT_MS` |

- Sai `ORACLE_CLIENT_LIB_DIR`.
- A senha nunca entra numa string de conexão logada.
- O expurgo usa o usuário `ohip_purge` (`sql/postgres/900_grants_example.sql`).

### 8. DDL

- Novos scripts em `sql/postgres/`: `001_schema.sql`, `002_seed_publisher.sql`, `003_seed_chain.sql` (com `-v chain_code=...`) e `900_grants_example.sql`.
  - Rodar como `ohip_owner` com `search_path=ohip` e `psql -v ON_ERROR_STOP=1 --single-transaction`.
  - Mesmas tabelas, colunas, nomes de constraints e índices de `sql/001`–`003`.
  - Os nomes são contrato da deduplicação (classificação do duplicado e log) e dos testes. A métrica `ohip_duplicates_total` tem só o rótulo `chain_code`. O texto `{constraint}` do ADR-0009 nunca foi implementado e fica corrigido por nota naquele ADR.
  - **Contrato de tabelas inalterado** (RNF-15): mesmas colunas e semântica; só os tipos são do PostgreSQL.
- Os scripts Oracle (`sql/001`–`003`, `900`) saem junto com o adapter Oracle, no fim da fase.
- Continua valendo: **o projeto nunca roda DDL em banco compartilhado** (homologação ou produção). Quem aplica é o DBA.

### 9. Testes

- **Mesmos cenários de contrato** de `tests/stores/`, rodando no fake em memória e num PostgreSQL de teste. O marcador `oracle` vira `postgres`.
- PostgreSQL de teste só com `TEST_POSTGRES_DSN` **e** `TEST_POSTGRES_DISPOSABLE=sim`:
  - os testes só fazem DML nas chains `ZZT*`, com as mesmas guardas de hoje;
  - o schema vem dos scripts de `sql/postgres/`, aplicados pelo alvo `make test-db`. Ele **sobe o próprio contêiner** (`docker run --rm`, nome aleatório, porta só em `127.0.0.1`, senha gerada na hora) e aplica a DDL **somente no DSN que ele mesmo criou**. Nunca aceita um DSN informado, porque `localhost` também pode ser túnel SSH, `port-forward` ou um PostgreSQL de verdade na mesma VM;
  - antes da DDL, o alvo confere que o banco está vazio (nenhuma relação no schema alvo) e aplica com `ON_ERROR_STOP=1 --single-transaction`;
  - no fim, imprime o `TEST_POSTGRES_DSN` para os testes e `make test-db-down` remove o contêiner. É DDL num contêiner descartável criado pelo próprio alvo, nunca em banco existente.
- **Unitários do adapter com driver falso roteirizado** (como `tests/fakes/oracle_driver.py`): ordem dos comandos, savepoints, rollback e classificação de erros, para o `make check` continuar sem infraestrutura e com cobertura ≥ 80%.
- **Uso de índice:** a 11.2 roda `EXPLAIN` de `RECENT_EVENTS_SQL` (`/status`) e do `DELETE` do expurgo com volume de teste e exige `Index Scan`/`Bitmap Index Scan` nos índices de data.
- **Erros raros provados no banco real sem DDL:** `DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '53100'; END $$` gera o disco cheio na chamada. O erro de recurso **por linha** continua provado só com o driver falso, a mesma limitação do ADR-0013.
- Teste de carga (`make test-load`) refeito com a latência medida no PostgreSQL de teste (`LOAD_DB_*`).

### 10. Camadas

- `.importlinter`: `psycopg` e `psycopg_pool` entram na lista proibida para `domain`/`application` e para o `ohip-admin` (ADR-0004), no lugar de `oracledb`.
- O pacote `adapters/oracle` vira `adapters/postgres`.

## Resultado da implementação (2026-10-21)

- `adapters/postgres/` substituiu `adapters/oracle/`, que foi removido junto com `sql/001`–`003`/`900`, `oracledb`, `OracleSettings` (`ORACLE_*`), `API_ORACLE_CALL_TIMEOUT_MS` e o marcador `oracle`.
- **Contrato da API e das métricas:** a chave da dependência no `/ready` e o rótulo `source` de `ohip_metrics_source_up` passaram de `oracle` para `postgres` (o nome antigo seria enganoso). A regra de alerta usa `ohip_metrics_source_up == 0` sem filtrar o rótulo, então não muda.
- O expurgo usa `PURGE_CALL_TIMEOUT_MS` como `statement_timeout` do seu pool (`open_pool(..., statement_timeout_ms=...)`); a API usa `API_DB_STATEMENT_TIMEOUT_MS` no pool de cada worker.
- **Testes:**
  - os 19 cenários de contrato de `tests/stores/` passam no fake e no PostgreSQL 16 do contêiner (`make test-db` + `make test-postgres`);
  - 21 testes só de banco real (`tests/stores/test_postgres_only.py`): classificação de 9 SQLSTATE e do `statement_timeout`, nenhuma transação aberta depois de leitura ou escrita, os testes 2, 4 e 5 do ADR-0022, as duas corridas reais (retry de DLQ e zumbi na barreira) e o uso de índice no `/status` e no expurgo (`EXPLAIN` com `enable_seqscan = off`);
  - unitários com driver falso (`tests/fakes/postgres_driver.py`) para sessões, transação, classificação e os dois caminhos do lote, inclusive o teste 3 do ADR-0022.
- **Carga** (latência medida no contêiner: lote de 200 com p95 de 26 ms no caminho rápido e 202 ms no lento): sustentado a 10× com p95 de 0,27 s (Fase 10, com Oracle simulado: 0,46 s); rajada a ~2 170 eventos/s; no pior caso, com todo lote no caminho lento, p95 de 0,26 s no sustentado e ~660 eventos/s na rajada.
- No caminho lento, o psycopg registra o aviso `error ignored terminating Pipeline` ao sair do pipeline abortado. É esperado e documentado no RUNBOOK.
- O `make check` não depende de banco (marcador `postgres` fora de `UNIT_MARKERS`). Os stores têm unitários com o driver falso (`test_pg_stores.py`: SQL, mapeamento de erros, barreira e o `merge_sql` do enricher); o banco real prova o resto.
- **`ON CONFLICT` do enricher executado antes da Q-1:** um teste cria uma tabela **TEMP** (some com a sessão) e roda o `INSERT ... ON CONFLICT DO UPDATE ... WHERE` contra o banco real. Só roda com `TEST_POSTGRES_LOCAL_CONTAINER=sim`, que só o `make test-db` grava: nunca num banco da empresa.
- **Inventário `''` × `NULL`** (§4): os opcionais do evento chegam normalizados pelo domínio (vazio → `None`); os opcionais gravados pelo adapter já viram `NULL` quando vazios (`last_close_reason`, `unique_event_id`/`offset_value` da DLQ); os demais textos gravados são obrigatórios. Nenhuma conversão extra foi necessária.
- **PgBouncer** (Q-18): em modo transação ele recusa as opções de conexão (`TimeZone`, `statement_timeout`) e o preparo automático de comandos do psycopg; se houver, ajustar a conexão antes de ligar.

## Consequências

- **Ganhos:**
  - validação do adapter com banco real na máquina do desenvolvedor (contêiner), sem esperar a Q-17;
  - instalação sem Instant Client;
  - somem as restrições do 11g (30 caracteres, sem `IDENTITY`/`LIMIT`/`RETURNING` em `INSERT ... SELECT`).
- **Riscos:**
  - a gravação do lote muda de mecanismo (ADR-0022): exige atenção do revisor e teste de carga;
  - `CACHE 1` em `ohip_outbox_seq` custa uma ida ao gerador por linha da outbox. É barato no PostgreSQL, mas o teste de carga mede.
- **Documentos que mudam nesta fase:**
  - CLAUDE.md (seção do banco, stack e camadas);
  - ARCHITECTURE §5 e §9;
  - RUNBOOK (instalação, testes, Instant Client);
  - `.env.example`;
  - units do systemd (comentários);
  - README;
  - PLAN e OPEN_QUESTIONS (Q-3, Q-11 obsoleta, Q-17 reescrita, Q-18 nova, DV-15).
- **Não muda:**
  - contrato da fila (v1), API, painel, protocolo OHIP, lease com epoch e outbox;
  - nomes e colunas das tabelas.
- **Validação:**
  - com banco real só quando houver PostgreSQL de teste com a mesma versão do de produção (Q-18);
  - o contêiner local prova o adapter, não o servidor da empresa.
