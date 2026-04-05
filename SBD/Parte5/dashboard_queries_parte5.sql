-- KPI 1: Numero total de transacciones (silver)
SELECT count(*) AS total_transactions
FROM iceberg.payments.silver_payments;

-- KPI 2: Numero total de alertas de fraude (gold)
SELECT count(*) AS total_fraud_alerts
FROM iceberg.payments.fraud_alerts;

-- Serie temporal: evolucion de alertas por hora
SELECT date_trunc('hour', event_time) AS hour_bucket, count(*) AS total_alerts
FROM iceberg.payments.fraud_alerts
GROUP BY 1
ORDER BY 1;

-- Ranking de comercios con mayor riesgo
SELECT merchant_id, avg(risk_score) AS avg_risk_score, count(*) AS alert_count
FROM iceberg.payments.fraud_alerts
GROUP BY 1
ORDER BY avg_risk_score DESC, alert_count DESC
LIMIT 20;

-- Ranking de tarjetas con mayor riesgo
SELECT card_id, avg(risk_score) AS avg_risk_score, count(*) AS alert_count
FROM iceberg.payments.fraud_alerts
GROUP BY 1
ORDER BY avg_risk_score DESC, alert_count DESC
LIMIT 20;
