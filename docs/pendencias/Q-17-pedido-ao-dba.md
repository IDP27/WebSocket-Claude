# Q-17 — Pedido ao DBA da Aviva: Oracle 11g de teste e VM

> Texto pronto para enviar ao DBA, seguido do checklist que o time roda quando o ambiente chegar. Os testes do projeto **nunca** rodam DDL nem apontam para homologação ou produção (CLAUDE.md).

## 1. O que pedimos

### 1.1 Schema descartável no Oracle 11g

| Item | Pedido |
| --- | --- |
| Banco | Oracle 11g, **fora** de homologação e produção (pode ser uma instância de desenvolvimento compartilhada) |
| Schema | Um usuário próprio e descartável (sugestão: `OHIP_TEST`), dono dos objetos. Os testes usam nomes sem prefixo de schema |
| Objetos | Aplicar, nesta ordem, `sql/001_sequences.sql`, `sql/002_core_tables.sql` e `sql/003_seed_leases.sql`. No `003`, rodar só a linha do lease `publisher` (o bloco "por chain" não é necessário: os testes semeiam as chains `ZZT1`/`ZZT2` sozinhos) |
| Privilégios | `CREATE SESSION` e cota no tablespace dos objetos. Nenhum privilégio de sistema além disso; os testes fazem só `SELECT/INSERT/UPDATE/DELETE` |
| Espaço | Pequeno: os testes gravam dezenas de linhas por execução. Para o teste de volume do checklist (§3, passo 4), algumas centenas de MB com CLOB |
| RAC | Informar se é RAC (Q-11: muda `NOORDER` → `ORDER` em `OHIP_OUTBOX_SEQ`) |
| Acesso | DSN (`host:porta/serviço`), usuário e senha entregues **fora do repositório** (cofre ou canal seguro) |
| Permissão para `EXPLAIN PLAN` | `PLAN_TABLE` acessível ao usuário (padrão do 11g) |

### 1.2 VM de teste

| Item | Pedido |
| --- | --- |
| SO | Linux x86_64 (o Instant Client 19 não existe para Mac arm64) |
| Software | Oracle Instant Client 19 **Basic** (a Basic Light só aceita alguns conjuntos de caracteres do banco), Python 3.11 a 3.13, Poetry, `make` e `git` |
| Rede | Rota até o listener do Oracle de teste. Opcional nesta etapa: saída para RabbitMQ e Redis de teste |
| Acesso | SSH para o time do projeto |

## 2. O que **não** pedimos

- Nada em homologação ou produção.
- Nenhum `CREATE ANY`, `DBA` ou acesso a outros schemas.
- Os usuários de produção (`OHIP_OWNER`, `OHIP_APP`, `OHIP_PURGE`, `OHIP_READ`) ficam para o deploy; o rascunho deles está em `sql/900_grants_example.sql`.

## 3. Checklist ao receber o ambiente

Rodar na VM, a partir do repositório, com as variáveis só na sessão (nunca em arquivo commitado):

```bash
read -rs TEST_ORACLE_PASSWORD && export TEST_ORACLE_PASSWORD   # senha fora do histórico do shell
export TEST_ORACLE_DSN=host:1521/servico TEST_ORACLE_USER=ohip_test
export TEST_ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_19_x TEST_ORACLE_DISPOSABLE_SCHEMA=sim
```

1. **Testes de integração** (Fase 3): `make test-integration`. Antes de qualquer DML, a suíte recusa o schema se encontrar chain que não seja `ZZT…` ou lease `publisher` ativo com outro dono.
2. **Planos das listagens da API** (ADR-0017): `EXPLAIN PLAN` de `/events` com `processing_status`, `module_name` e janela larga; `/outbox?status=FAILED` sem chain; status com `OUTBOX_COUNTS_SQL` e `PERCENTILE_CONT`. Com volume realista (passo 4).
3. **Expurgo** (ADR-0020): `EXPLAIN PLAN` dos `DELETE` de `adapters/oracle/purge_store.py` e um `ohip-purge` só contando. O processo lê `ORACLE_*`, não `TEST_ORACLE_*`:

   ```bash
   export ORACLE_DSN="$TEST_ORACLE_DSN" ORACLE_USER="$TEST_ORACLE_USER" ORACLE_PASSWORD="$TEST_ORACLE_PASSWORD"
   export ORACLE_CLIENT_LIB_DIR="$TEST_ORACLE_CLIENT_LIB_DIR" APP_ENVIRONMENT=desenvolvimento PURGE_DRY_RUN=true
   ohip-purge
   ```
4. **Desempenho** (ADR-0020 §3): medir o tempo de um lote de 200 eventos no adapter, ajustar `LOAD_DB_*` e rodar `make test-load`; com a Q-8 respondida, ajustar `LOAD_RATE_PER_S`.
5. **Instant Client nas units** (RUNBOOK): conferir onde ele grava `sqlnet.log`/`oradiag_*` com `ProtectSystem=strict`; se não for `/var/lib/ohip`, apontar `TNS_ADMIN`/`ADR_BASE`/`LOG_DIRECTORY_CLIENT`.
6. Registrar o resultado na Q-17 (`docs/OPEN_QUESTIONS.md`) e marcar a validação da Fase 3 no `docs/PLAN.md`.

Ao final, o DBA pode apagar o schema inteiro: nada dele é reaproveitado.
