#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

echo "==> Verificando Trino + Iceberg"
docker compose up -d trino >/dev/null

echo
echo "==> Esquemas del catalogo iceberg"
docker compose exec -T trino trino --execute "SHOW SCHEMAS FROM iceberg"

echo
echo "==> Tablas en iceberg.payments"
docker compose exec -T trino trino --execute "SHOW TABLES FROM iceberg.payments"

echo
echo "==> Conteo total de transacciones (silver)"
docker compose exec -T trino trino --execute "SELECT count(*) AS total_transactions FROM iceberg.payments.silver_payments"

echo
echo "==> Conteo total de alertas (gold)"
docker compose exec -T trino trino --execute "SELECT count(*) AS total_fraud_alerts FROM iceberg.payments.fraud_alerts"

echo
echo "==> Evolucion temporal de alertas"
docker compose exec -T trino trino --execute "
SELECT date_trunc('hour', event_time) AS hour_bucket, count(*) AS total_alerts
FROM iceberg.payments.fraud_alerts
GROUP BY 1
ORDER BY 1
LIMIT 24
"

echo
echo "==> Comercios con mayor riesgo"
docker compose exec -T trino trino --execute "
SELECT merchant_id, avg(risk_score) AS avg_risk_score, count(*) AS alert_count
FROM iceberg.payments.fraud_alerts
GROUP BY 1
ORDER BY avg_risk_score DESC, alert_count DESC
LIMIT 10
"

echo
echo "==> Tarjetas con mayor riesgo"
docker compose exec -T trino trino --execute "
SELECT card_id, avg(risk_score) AS avg_risk_score, count(*) AS alert_count
FROM iceberg.payments.fraud_alerts
GROUP BY 1
ORDER BY avg_risk_score DESC, alert_count DESC
LIMIT 10
"
