# Parte 11 - Consultas y análisis con Athena

Scripts:
- `SBD/Parte11/athena_analysis_parte11.py`
- `SBD/Parte11/run_parte11.sh`
- `SBD/Parte11/athena_queries_parte11.sql` (SQL para Athena en AWS)

Funcionalidad:
1. Lee el dataset filtrado/consolidado de Parte10 desde S3/MinIO.
2. Ejecuta análisis tipo Athena para:
   - transacciones de alto riesgo
   - tarjetas con actividad sospechosa
   - patrones geográficos de fraude
3. Guarda resultados en S3/MinIO en CSV o Parquet para dashboards/alertas.

Datasets de salida:
- `high_risk_transactions`
- `suspicious_cards`
- `geo_fraud_patterns`
- `summary`

Cómo ejecutar (local):
```bash
./SBD/Parte11/run_parte11.sh \
  --input-path s3a://lakehouse/analytics/parte10/high_risk_payments \
  --output-path s3a://lakehouse/analytics/parte11/results \
  --output-format parquet \
  --output-mode overwrite
```

Ejemplo en CSV:
```bash
./SBD/Parte11/run_parte11.sh --output-format csv --output-mode overwrite
```

Parámetros relevantes:
- `--high-risk-threshold` (default 70)
- `--min-high-risk-tx` (default 2)
- `--tx5m-threshold` (default 8)
- `--countries-threshold` (default 2)
- `--geo-countries1h-threshold` (default 2)
- `--suspicious-countries` (default `RU,NG,KP,IR`)

Variables útiles:
- `PARTE11_INPUT_PATH`
- `PARTE11_OUTPUT_PATH`
- `PARTE11_OUTPUT_FORMAT`
- `PARTE11_OUTPUT_MODE`
- `PARTE11_HIGH_RISK_THRESHOLD`
- `PARTE11_MIN_HIGH_RISK_TX`
- `PARTE11_TX5M_THRESHOLD`
- `PARTE11_COUNTRIES_THRESHOLD`
- `PARTE11_GEO_COUNTRIES1H_THRESHOLD`
- `PARTE11_SUSPICIOUS_COUNTRIES`

Nota:
- En local se usa MinIO como capa S3 compatible.
- En AWS, puedes usar `athena_queries_parte11.sql` para ejecutar las mismas consultas con CTAS y persistir en S3.
