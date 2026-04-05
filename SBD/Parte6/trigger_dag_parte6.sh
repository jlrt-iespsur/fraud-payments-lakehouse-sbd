#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

# configurar según hagamos pruebas o lo dejemos como definitivos
SOURCE_TABLE="${SOURCE_TABLE:-graph_payments}"
START_TS="${START_TS:-}"
END_TS="${END_TS:-}"
GRAPH_NAME="${GRAPH_NAME:-fraud_snapshot_sbd}"

CONF=$(cat <<EOF
{
  "source_table": "${SOURCE_TABLE}",
  "start_ts": "${START_TS}",
  "end_ts": "${END_TS}",
  "graph_name": "${GRAPH_NAME}"
}
EOF
)

docker compose exec -T airflow-webserver airflow dags trigger fraud_graph_pipeline_on_demand_sbd --conf "${CONF}"
