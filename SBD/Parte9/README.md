# Parte 9 - S3 (Silver) -> RDS MySQL

Script:
- `SBD/Parte9/silver_s3_to_rds_mysql_parte9.py`
- `SBD/Parte9/run_parte9.sh`

Funcionalidad:
1. Lee datos desde Silver en S3/MinIO (formato Parquet).
2. Reaplica tipado de columnas y features.
3. Inserta los datos en MySQL (RDS) por JDBC.

Cómo ejecutarlo (local con Docker Compose):
```bash
./SBD/Parte9/run_parte9.sh \
  --source-path s3a://lakehouse/warehouse/payments/silver_payments/data \
  --jdbc-url "jdbc:mysql://mysql:3306/fraud?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" \
  --jdbc-user admin \
  --jdbc-password admin \
  --target-database fraud \
  --target-table silver_features \
  --mode append
```

Uso con RDS en AWS:
```bash
./SBD/Parte9/run_parte9.sh \
  --source-path s3a://<bucket>/<ruta_silver>/data \
  --jdbc-url "jdbc:mysql://<rds-endpoint>:3306/fraud?useSSL=true&serverTimezone=UTC" \
  --jdbc-user <usuario> \
  --jdbc-password '<password>' \
  --target-database fraud \
  --target-table silver_features \
  --mode append
```

Probar carga completa reemplazando tabla destino:
```bash
./SBD/Parte9/run_parte9.sh --mode overwrite
```

Variables útiles:
- `SILVER_S3_PATH`
- `S3_ENDPOINT`
- `S3_ACCESS_KEY` / `S3_SECRET_KEY`
- `S3_PATH_STYLE_ACCESS`
- `MYSQL_JDBC_URL`
- `MYSQL_USER` / `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `MYSQL_TARGET_TABLE`
- `MYSQL_WRITE_MODE`

Nota:
- En local, MinIO simula S3 de forma transparente.
- En producción, solo hay que cambiar variables (sin cambios de código).
