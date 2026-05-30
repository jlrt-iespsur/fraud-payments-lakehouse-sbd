#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

docker compose exec generator python /opt/project/SBD/Parte12/fine_tuning_hf_parte12.py "$@"
