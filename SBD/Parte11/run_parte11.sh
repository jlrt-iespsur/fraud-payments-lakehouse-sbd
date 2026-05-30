#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

docker compose exec \
  -e AWS_REGION="${AWS_REGION:-us-east-1}" \
  -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
  spark spark-submit \
  --master local[2] \
  --packages "$PACKAGES" \
  /opt/project/SBD/Parte11/athena_analysis_parte11.py "$@"
