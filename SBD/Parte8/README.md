# Parte 8 - Exportación a S3 (Python + boto3)

Script:
- `SBD/Parte8/export_gold_to_s3_parte8.py`

Funcionalidad:
1. Lee datos desde Gold (Trino + Iceberg)
2. Genera ficheros JSON o Parquet
3. Sube los ficheros a S3 (o MinIO)
4. Organiza por particiones temporales:
   - `gold/year=YYYY/month=MM/day=DD/fraud_alerts.json`

Cómo ejecutarlo (usando JSON):
```bash
python3 SBD/Parte8/export_gold_to_s3_parte8.py \
  --source-table iceberg.payments.fraud_alerts \
  --format json \
  --bucket lakehouse \
  --prefix gold \
  --filename fraud_alerts \
  --partition-column event_time \
  --start-trino
```

Uso con filtro temporal:
```bash
python3 SBD/Parte8/export_gold_to_s3_parte8.py \
  --source-table iceberg.payments.fraud_alerts \
  --start-ts 2026-03-18T00:00:00Z \
  --end-ts 2026-03-19T00:00:00Z \
  --format json \
  --bucket lakehouse \
  --prefix gold \
  --start-trino
```

Probar script sin subir a S3:
```bash
python3 SBD/Parte8/export_gold_to_s3_parte8.py --dry-run --start-trino
```

Variables útiles:
- `S3_ENDPOINT_URL` (default fallback: `http://localhost:9000`)
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
- `S3_BUCKET` o `MINIO_BUCKET`

Nota:
- Para subir ficheros hay que tener `boto3` instalado en tu Python local.
- Para `--format parquet` se necesita también `pyarrow` (o `fastparquet`).
