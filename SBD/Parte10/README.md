# Parte 10 - ELT unión S3 + RDS

Script:
- `SBD/Parte10/s3_rds_union_parte10.py`
- `SBD/Parte10/run_parte10.sh`

Funcionalidad:
1. Lee dataset desde S3/MinIO (Silver o Gold en Parquet).
2. Lee dataset desde RDS MySQL (JDBC).
3. Limpia y normaliza campos clave:
   - `payment_id`: trim + string
   - `card_id`: trim + mayúsculas
   - `amount`: double, elimina nulos/negativos
   - `event_time`: parseo a timestamp (ISO y variantes)
   - `country`: normaliza códigos (ES/es/España -> ES, etc.)
   - `status`: normaliza a `approved` / `declined`
4. Elimina duplicados por `payment_id`.
5. Maneja nulos:
   - `risk_score` -> 0 si nulo
   - `tx_5m`, `merchants_10m`, `countries_1h` -> 0 si nulo
6. Une ambos datasets por `payment_id`.
7. Aplica filtros:
   - riesgo alto
   - frecuencia transaccional
   - geografía sospechosa
8. Guarda resultado en S3/MinIO (Parquet particionado por `event_date`, listo para Athena).

Cómo ejecutarlo:
```bash
./SBD/Parte10/run_parte10.sh \
  --s3-input-path s3a://lakehouse/warehouse/payments/silver_payments/data \
  --mysql-jdbc-url "jdbc:mysql://mysql:3306/fraud?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" \
  --mysql-user admin \
  --mysql-password admin \
  --mysql-database fraud \
  --mysql-table silver_features \
  --output-path s3a://lakehouse/analytics/parte10/high_risk_payments \
  --output-mode overwrite
```

Parámetros de filtro:
- `--high-risk-threshold` (default 60)
- `--tx5m-threshold` (default 5)
- `--merchants10m-threshold` (default 4)
- `--countries1h-threshold` (default 3)
- `--suspicious-countries` (default `RU,NG,KP,IR`)

Nota:
- En local usa MinIO; en producción solo cambia variables/paths para S3 y RDS.
