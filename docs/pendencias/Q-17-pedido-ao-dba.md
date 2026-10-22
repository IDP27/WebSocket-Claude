# Q-17 / Q-18 — Pedido ao DBA: PostgreSQL de teste

> Texto pronto para enviar ao DBA ou à infraestrutura responsável pelo PostgreSQL, seguido do checklist que o time roda quando o ambiente chegar. Reescrito em 2026-10-21 com a troca do Oracle 11g pelo PostgreSQL (DV-15, ADR-0021). O adapter já é provado num contêiner local descartável (`make test-db`); este pedido serve para repetir a prova **no servidor da empresa, com a versão de produção**. Os testes do projeto **nunca** rodam DDL em banco compartilhado nem apontam para homologação ou produção (CLAUDE.md).

## 1. O que precisamos saber (Q-18)

| Pergunta | Por que importa |
| --- | --- |
| Qual a **versão** do PostgreSQL de produção? | O projeto exige 15 ou superior (ADR-0021) e foi testado no 16 |
| Onde ele roda: Aviva, serviço gerenciado (qual) ou outro? | TLS, rede, backup, limites de conexão |
| Quem é o **DBA responsável**? | Aplicação da DDL, papéis, planos de execução |
| O certificado TLS do servidor é de uma CA do sistema ou de uma CA interna? | `POSTGRES_SSLMODE=verify-full` e, se interna, `POSTGRES_SSLROOTCERT` |
| Há limite de conexões por papel, `idle_in_transaction_session_timeout`, `statement_timeout` de servidor ou pooler (PgBouncer) na frente? | O pool de cada processo (`POSTGRES_POOL_MAX`) e os prazos precisam caber nesses limites. Um PgBouncer em modo transação recusa as opções de conexão que o projeto envia (`TimeZone`, `statement_timeout`) e o preparo automático de comandos do psycopg: nesse caso, ajustamos a conexão antes de ligar |

## 2. O que pedimos

### 2.1 Banco de teste

| Item | Pedido |
| --- | --- |
| Servidor | PostgreSQL **com a mesma versão de produção**, fora de homologação e produção (pode ser uma instância de desenvolvimento compartilhada) |
| Banco e schema | Um banco (ou schema `ohip`) descartável, só para o projeto |
| Objetos | Aplicar, nesta ordem, como dono do schema e com `search_path=ohip`: `sql/postgres/001_schema.sql` e `002_seed_publisher.sql`, com `psql -v ON_ERROR_STOP=1 --single-transaction -f <arquivo>`. O `003_seed_chain.sql` não é necessário: os testes semeiam as chains `ZZT1`/`ZZT2` sozinhos |
| Papel de teste | Um papel com `LOGIN`, `USAGE` no schema, `SELECT/INSERT/UPDATE/DELETE` nas 7 tabelas e `USAGE` nas 4 sequências. Nenhum privilégio de DDL nem de superusuário |
| Espaço | Pequeno: dezenas de linhas por execução. Para o teste de volume do checklist (§4, passo 4), algumas centenas de MB |
| Acesso | Host, porta, banco, usuário e senha entregues **fora do repositório** (cofre ou canal seguro); TLS habilitado |

### 2.2 Rede

- Rota da máquina do time (ou de uma VM de teste) até a porta do PostgreSQL de teste.
- Opcional nesta etapa: RabbitMQ e Redis de teste da empresa (os testes já passaram em contêineres locais).

## 3. O que **não** pedimos

- Nada em homologação ou produção.
- Nenhum superusuário, `CREATEDB`, `CREATEROLE` ou acesso a outros schemas.
- Os papéis de produção (`ohip_owner`, `ohip_app`, `ohip_purge`, `ohip_read`) ficam para o deploy; o rascunho está em `sql/postgres/900_grants_example.sql`.
- Instant Client, cliente de banco ou bibliotecas no servidor da aplicação: o driver (`psycopg[binary]`) já traz a libpq.

## 4. Checklist ao receber o ambiente

Rodar a partir do repositório, com as variáveis só na sessão (nunca em arquivo commitado):

```bash
read -rs PGPASS && export TEST_POSTGRES_DSN="host=<host> port=5432 dbname=<banco> user=<papel> password=$PGPASS sslmode=verify-full"
export TEST_POSTGRES_DISPOSABLE=sim
```

1. **Cenários e testes de banco real** (Fase 11): `make test-postgres` (sem o `make test-db`, que é só para o contêiner local). Antes de qualquer DML, a suíte recusa o banco se encontrar chain que não seja `ZZT…` ou lease `publisher` ativo com outro dono.
2. **Planos das listagens da API** (ADR-0017), com volume realista (passo 4) e **sem** `enable_seqscan = off`: `EXPLAIN (ANALYZE, BUFFERS)` de `/events` com `processing_status`, `module_name` e janela larga; `/outbox?status=FAILED` sem chain; `/status` (`RECENT_EVENTS_SQL` e `OUTBOX_COUNTS_SQL`).
3. **Expurgo** (ADR-0020): `EXPLAIN` dos `DELETE` de `adapters/postgres/purge_store.py` e um `ohip-purge` só contando. O processo lê `POSTGRES_*`, não `TEST_POSTGRES_*`:

   ```bash
   export POSTGRES_HOST=<host> POSTGRES_DB=<banco> POSTGRES_USER=<papel> POSTGRES_PASSWORD="$PGPASS"
   export APP_ENVIRONMENT=desenvolvimento PURGE_DRY_RUN=true
   ohip-purge
   ```
4. **Desempenho** (ADR-0020 §3, ADR-0022 §3): medir o lote de 200 eventos no adapter (caminho rápido e lento), ajustar `LOAD_DB_*` e rodar `make test-load`; com a Q-8 respondida, ajustar `LOAD_RATE_PER_S`. Referência do contêiner local: p95 de 26 ms (rápido) e 202 ms (lento) por lote.
5. Registrar o resultado na Q-17 e na Q-18 (`docs/OPEN_QUESTIONS.md`) e marcar a validação no `docs/PLAN.md`.

Ao final, o DBA pode apagar o banco ou o schema inteiro: nada dele é reaproveitado.
