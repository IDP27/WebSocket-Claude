#!/usr/bin/env bash
# PostgreSQL descartável para os testes (ADR-0021 §9).
#
#   scripts/test_db.sh up     cria um contêiner novo, aplica sql/postgres/ e grava .test-db.env
#   scripts/test_db.sh down   remove o contêiner criado pelo "up"
#
# Regra do projeto: DDL nunca em banco existente. Este script só aplica a DDL no contêiner
# que ele mesmo acabou de criar (nome aleatório, porta só em 127.0.0.1, senha gerada na
# hora) e nunca aceita um DSN informado. Confere que o banco está vazio antes da DDL e roda
# cada arquivo com ON_ERROR_STOP numa transação única.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STATE="$ROOT/.test-db"          # nome do contêiner criado
ENV_FILE="$ROOT/.test-db.env"   # TEST_POSTGRES_* para os testes (fora do git)
IMAGE="${TEST_DB_IMAGE:-postgres:16}"
DB=ohip_test
ROLE=ohip_test

down() {
    if [[ -f "$STATE" ]]; then
        docker rm -f "$(cat "$STATE")" >/dev/null 2>&1 || true
        rm -f "$STATE" "$ENV_FILE"
        echo "contêiner de teste removido"
    else
        echo "nenhum contêiner de teste criado por este script"
    fi
}

psql_in() {  # psql dentro do contêiner, como o papel informado
    local user="$1"; shift
    docker exec -i "$NAME" psql -U "$user" -d "$DB" -v ON_ERROR_STOP=1 -q "$@"
}

up() {
    if [[ -f "$STATE" ]]; then
        echo "já existe um contêiner de teste ($(cat "$STATE")); rode 'make test-db-down' antes" >&2
        exit 1
    fi
    NAME="ohip-test-db-$(openssl rand -hex 4)"
    local password
    password="$(openssl rand -hex 16)"
    docker run -d --rm --name "$NAME" \
        -e POSTGRES_PASSWORD="$password" -e POSTGRES_DB="$DB" \
        -p 127.0.0.1::5432 "$IMAGE" >/dev/null
    echo "$NAME" > "$STATE"
    trap 'echo "falhou; removendo $NAME" >&2; down >&2' ERR

    for _ in $(seq 1 60); do
        docker exec "$NAME" pg_isready -h 127.0.0.1 -U postgres -d "$DB" -q 2>/dev/null && break
        sleep 1
    done
    # -h 127.0.0.1: o servidor temporário do initdb só escuta no socket Unix, então o TCP só
    # responde quando o servidor definitivo está de pé.

    # Banco recém-criado tem que estar vazio: nenhuma relação fora dos schemas do sistema.
    local relations
    relations="$(psql_in postgres -Atc "SELECT count(*) FROM pg_class c JOIN pg_namespace n
        ON n.oid = c.relnamespace WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND n.nspname NOT LIKE 'pg_toast%'")"
    if [[ "$relations" != "0" ]]; then
        echo "o banco do contêiner não está vazio ($relations relações); nada foi aplicado" >&2
        exit 1
    fi

    psql_in postgres -c "CREATE ROLE $ROLE LOGIN PASSWORD '$password';
        CREATE SCHEMA ohip AUTHORIZATION $ROLE;
        ALTER ROLE $ROLE SET search_path = ohip;"
    for script in 001_schema.sql 002_seed_publisher.sql; do
        psql_in "$ROLE" --single-transaction < "$ROOT/sql/postgres/$script"
    done

    local port
    port="$(docker port "$NAME" 5432/tcp | head -1 | sed 's/.*://')"
    umask 077
    cat > "$ENV_FILE" <<EOF
TEST_POSTGRES_DSN='host=127.0.0.1 port=$port dbname=$DB user=$ROLE password=$password sslmode=disable'
TEST_POSTGRES_DISPOSABLE=sim
# Só o contêiner criado aqui: libera a tabela TEMP do teste do ON CONFLICT do enricher.
TEST_POSTGRES_LOCAL_CONTAINER=sim
EOF
    trap - ERR
    echo "PostgreSQL de teste pronto: $NAME em 127.0.0.1:$port (variáveis em .test-db.env)"
}

case "${1:-}" in
    up) up ;;
    down) down ;;
    *) echo "uso: $0 up|down" >&2; exit 2 ;;
esac
