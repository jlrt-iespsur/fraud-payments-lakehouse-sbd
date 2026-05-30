-- PARTE 11 - Consultas Athena sobre salida de Parte10 en S3
--
-- Ajusta estos nombres antes de ejecutar en Athena:
--   <database_name>
--   <source_table>
--   <results_s3_prefix>
--
-- Recomendado: crear una tabla externa en Glue/Athena para la salida de Parte10
-- apuntando al prefijo S3 de Parte10 (Parquet particionado por event_date).

-- 1) Transacciones de alto riesgo
CREATE TABLE <database_name>.parte11_high_risk_transactions
WITH (
  format = 'PARQUET',
  external_location = 's3://<results_s3_prefix>/high_risk_transactions/'
) AS
SELECT
  payment_id,
  card_id,
  amount,
  event_time,
  country,
  status,
  risk_score,
  tx_5m,
  merchants_10m,
  countries_1h,
  event_date
FROM <database_name>.<source_table>
WHERE risk_score >= 70 OR lower(status) = 'declined';

-- 2) Tarjetas con actividad sospechosa
CREATE TABLE <database_name>.parte11_suspicious_cards
WITH (
  format = 'PARQUET',
  external_location = 's3://<results_s3_prefix>/suspicious_cards/'
) AS
SELECT
  card_id,
  count(*) AS total_tx,
  sum(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) AS high_risk_tx,
  avg(risk_score) AS avg_risk_score,
  max(tx_5m) AS max_tx_5m,
  count(DISTINCT country) AS distinct_countries
FROM <database_name>.<source_table>
GROUP BY card_id
HAVING sum(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) >= 2
    OR max(tx_5m) >= 8
    OR count(DISTINCT country) >= 2;

-- 3) Patrones geográficos de fraude
CREATE TABLE <database_name>.parte11_geo_fraud_patterns
WITH (
  format = 'PARQUET',
  external_location = 's3://<results_s3_prefix>/geo_fraud_patterns/'
) AS
SELECT
  country,
  date_trunc('hour', event_time) AS hour_bucket,
  count(*) AS total_tx,
  sum(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) AS high_risk_tx,
  avg(risk_score) AS avg_risk_score,
  count(DISTINCT card_id) AS distinct_cards
FROM <database_name>.<source_table>
GROUP BY country, date_trunc('hour', event_time)
HAVING sum(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) > 0
ORDER BY high_risk_tx DESC, avg_risk_score DESC;
