#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.postgresql:postgresql:42.7.3"

docker compose exec \
  -T \
  -e AWS_REGION=us-east-1 \
  -e AWS_DEFAULT_REGION=us-east-1 \
  spark spark-submit \
  --master local[*] \
  --packages "$PACKAGES" \
  /opt/project/SBD/Parte2/bronze_kafka_to_iceberg.py "$@"
