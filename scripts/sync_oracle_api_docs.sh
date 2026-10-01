#!/usr/bin/env bash
# Atualiza a cópia local das especificações oficiais da Oracle (ADR-0012).
#
# Uso:
#   scripts/sync_oracle_api_docs.sh --check            confere os arquivos com o SHA256SUMS
#   scripts/sync_oracle_api_docs.sh <commit>           baixa os arquivos do commit e regrava
#
# Depois de atualizar: rodar `make check` (os testes de contrato comparam o código com o
# schema novo), revisar o diff e registrar o commit novo em vendor/.../PROVENANCE.md.
set -euo pipefail

REPO="oracle/hospitality-api-docs"
DEST="$(cd "$(dirname "$0")/.." && pwd)/vendor/oracle-hospitality-api-docs"
FILES=(
  LICENSE.txt
  graphql/streaming/StreamingGraphQLSchema.json
  graphql/streaming/graphiql.html
  rest-api-specs/security/v1/publishedoauth.json
  rest-api-specs/property/v1/int.json
  rest-api-specs/property/v1/rsv.json
  rest-api-specs/property/v1/crm.json
)

if [[ "${1:-}" == "--check" ]]; then
  cd "$DEST" && shasum -a 256 -c SHA256SUMS
  exit $?
fi

COMMIT="${1:?informe o commit (40 caracteres) do repositório $REPO ou --check}"
if [[ ! "$COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
  echo "commit inválido: use o hash completo de 40 caracteres" >&2
  exit 2
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
curl -fsSL "https://codeload.github.com/$REPO/tar.gz/$COMMIT" -o "$WORK/repo.tar.gz"
tar -xzf "$WORK/repo.tar.gz" -C "$WORK"
SRC="$WORK/hospitality-api-docs-$COMMIT"

for file in "${FILES[@]}"; do
  mkdir -p "$DEST/$(dirname "$file")"
  cp "$SRC/$file" "$DEST/$file"
done
(cd "$DEST" && printf '%s\n' "${FILES[@]/#/./}" | sort | xargs shasum -a 256 > SHA256SUMS)
echo "Atualizado para $COMMIT. Registre o commit em $DEST/PROVENANCE.md e rode make check."
