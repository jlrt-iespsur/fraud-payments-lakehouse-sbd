#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,mysql:mysql-connector-java:8.0.33"

docker compose exec \
  -e AWS_REGION="${AWS_REGION:-us-east-1}" \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
  spark spark-submit \
  --master local[*] \
  --packages "$PACKAGES" \
  /opt/project/SBD/Parte10/s3_rds_union_parte10.py "$@"
