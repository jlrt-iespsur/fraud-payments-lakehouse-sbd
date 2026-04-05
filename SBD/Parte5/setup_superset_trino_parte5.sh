#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

DB_NAME="${DB_NAME:-trino_iceberg}"
TRINO_URI="${TRINO_URI:-trino://trino@trino:8080/iceberg/payments}"

echo "==> Levantando Trino y Superset"
docker compose up -d trino superset >/dev/null

echo
echo "==> Configurando conexion en Superset"
docker compose exec -T superset superset set-database-uri -d "$DB_NAME" -u "$TRINO_URI"

echo
echo "==> Probando conexion (driver + dialecto + conectividad)"
printf 'n\n' | docker compose exec -T superset superset test-db "$TRINO_URI"

echo
echo "Conexion lista en Superset:"
echo "- Nombre DB: $DB_NAME"
echo "- URI: $TRINO_URI"
