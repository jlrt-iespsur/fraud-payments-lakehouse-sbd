#!/usr/bin/env python3

# PARTE 8 - Exportacion de GOLD a S3 (Python + boto3)

import argparse
import csv
import io
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


root_dir = Path(__file__).resolve().parents[2]
os.chdir(root_dir)

parser = argparse.ArgumentParser(
    description="Lee datos de GOLD, genera JSON/Parquet y los sube a S3 con particiones temporales",
)
parser.add_argument(
    "--source-table",
    default=os.getenv("SOURCE_TABLE", "iceberg.payments.fraud_alerts"),
    help="Tabla GOLD origen (catalog.schema.table o schema.table o table)",
)
parser.add_argument(
    "--format",
    choices=["json", "parquet"],
    default=os.getenv("EXPORT_FORMAT", "json"),
    help="Formato de salida: json o parquet",
)
parser.add_argument(
    "--bucket",
    default=os.getenv("S3_BUCKET", os.getenv("MINIO_BUCKET", "lakehouse")),
    help="Bucket destino",
)
parser.add_argument(
    "--prefix",
    default=os.getenv("S3_PREFIX", "gold"),
    help="Prefijo destino en S3",
)
parser.add_argument(
    "--partition-column",
    default=os.getenv("PARTITION_COLUMN", "event_time"),
    help="Columna temporal para particionar",
)
parser.add_argument(
    "--filename",
    default=os.getenv("EXPORT_FILENAME", "fraud_alerts"),
    help="Nombre base del fichero (sin extension)",
)
parser.add_argument(
    "--start-ts",
    default=os.getenv("START_TS", ""),
    help="Filtro opcional ISO8601 inicio, ejemplo: 2026-03-18T00:00:00Z",
)
parser.add_argument(
    "--end-ts",
    default=os.getenv("END_TS", ""),
    help="Filtro opcional ISO8601 fin, ejemplo: 2026-03-19T00:00:00Z",
)
parser.add_argument(
    "--limit",
    type=int,
    default=int(os.getenv("LIMIT", "0")),
    help="Limite opcional de filas para prueba (0 = sin limite)",
)
parser.add_argument(
    "--output-dir",
    default=os.getenv("OUTPUT_DIR", "runtime/exports/parte8_s3"),
    help="Directorio local temporal donde se generan los ficheros",
)
parser.add_argument(
    "--trino-service",
    default=os.getenv("TRINO_SERVICE", "trino"),
    help="Nombre del servicio Trino en docker compose",
)
parser.add_argument(
    "--start-trino",
    action="store_true",
    help="Levanta Trino antes de consultar (recomendado)",
)
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Genera ficheros locales pero no sube nada a S3",
)
args = parser.parse_args()

identifier_pattern = re.compile(r"^[A-Za-z0-9_]+$")

table_raw = args.source_table.strip()
if table_raw == "":
    raise SystemExit("source_table no puede estar vacio")

parts = table_raw.split(".")
default_catalog = os.getenv("TRINO_CATALOG", "iceberg")
default_schema = os.getenv("TRINO_SCHEMA", "payments")

if len(parts) == 1:
    table_catalog = default_catalog
    table_schema = default_schema
    table_name = parts[0]
elif len(parts) == 2:
    table_catalog = default_catalog
    table_schema = parts[0]
    table_name = parts[1]
elif len(parts) == 3:
    table_catalog = parts[0]
    table_schema = parts[1]
    table_name = parts[2]
else:
    raise SystemExit(f"source_table invalido: {args.source_table}")

for label, value in [
    ("catalog", table_catalog),
    ("schema", table_schema),
    ("table", table_name),
    ("partition_column", args.partition_column),
]:
    if not identifier_pattern.fullmatch(value):
        raise SystemExit(f"{label} invalido: {value}")

source_table_fqn = f"{table_catalog}.{table_schema}.{table_name}"

where_predicates = []
if args.start_ts.strip():
    where_predicates.append(
        f"CAST({args.partition_column} AS TIMESTAMP WITH TIME ZONE) >= from_iso8601_timestamp('{args.start_ts.strip()}')"
    )
if args.end_ts.strip():
    where_predicates.append(
        f"CAST({args.partition_column} AS TIMESTAMP WITH TIME ZONE) < from_iso8601_timestamp('{args.end_ts.strip()}')"
    )

where_clause = ""
if where_predicates:
    where_clause = " WHERE " + " AND ".join(where_predicates)

limit_clause = ""
if args.limit > 0:
    limit_clause = f" LIMIT {args.limit}"

sql = f"SELECT * FROM {source_table_fqn}{where_clause}{limit_clause}"

if args.start_trino:
    print(f"Levantando {args.trino_service} ...")
    subprocess.run(
        ["docker", "compose", "up", "-d", args.trino_service],
        check=True,
    )

ready = False
for _ in range(30):
    check = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            args.trino_service,
            "trino",
            "--execute",
            "SELECT 1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if check.returncode == 0:
        ready = True
        break
    time.sleep(2)

if not ready:
    raise SystemExit("Trino no arranco a tiempo.")

print("Consultando GOLD en Trino ...")
result = subprocess.run(
    [
        "docker",
        "compose",
        "exec",
        "-T",
        args.trino_service,
        "trino",
        "--output-format",
        "CSV_HEADER",
        "--execute",
        sql,
    ],
    check=True,
    capture_output=True,
    text=True,
)

rows = []
csv_lines = []
for line in result.stdout.splitlines():
    stripped = line.strip()
    if stripped.startswith('"'):
        csv_lines.append(stripped)

if csv_lines:
    csv_text = "\n".join(csv_lines)
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = [dict(row) for row in reader]

if len(rows) == 0:
    print("No hay filas para exportar con esos filtros.")
    raise SystemExit(0)

print(f"Filas leidas: {len(rows)}")

partitions = {}
for row in rows:
    raw_ts = row.get(args.partition_column)
    if raw_ts is None or str(raw_ts).strip() == "":
        continue

    ts_text = str(raw_ts).strip()
    dt = None

    try:
        dt = datetime.strptime(ts_text, "%Y-%m-%d %H:%M:%S.%f UTC").replace(tzinfo=timezone.utc)
    except ValueError:
        pass

    if dt is None:
        try:
            dt = datetime.strptime(ts_text, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    if dt is None:
        try:
            dt = datetime.fromisoformat(ts_text.replace("Z", "+00:00"))
        except ValueError:
            pass

    if dt is None:
        continue

    year = f"{dt.year:04d}"
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"
    key = (year, month, day)

    if key not in partitions:
        partitions[key] = []
    partitions[key].append(row)

if len(partitions) == 0:
    print("No se pudieron generar particiones temporales con la columna indicada.")
    raise SystemExit(1)

output_root = (root_dir / args.output_dir).resolve()
output_root.mkdir(parents=True, exist_ok=True)

local_files = []
for (year, month, day), partition_rows in partitions.items():
    partition_dir = output_root / args.prefix / f"year={year}" / f"month={month}" / f"day={day}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    extension = "json" if args.format == "json" else "parquet"
    local_path = partition_dir / f"{args.filename}.{extension}"

    if args.format == "json":
        with local_path.open("w", encoding="utf-8") as handle:
            json.dump(partition_rows, handle, ensure_ascii=False, indent=2, default=str)
    else:
        try:
            import pandas as pd
        except Exception as exc:
            raise SystemExit(
                "No se pudo generar Parquet. No se encuentra pandas.\n"
                "Ejemplo: pip install pandas pyarrow"
            ) from exc

        dataframe = pd.DataFrame(partition_rows)
        try:
            dataframe.to_parquet(local_path, index=False)
        except Exception as exc:
            raise SystemExit(
                "No se pudo generar Parquet. Revisar pyarrow/fastparquet.\n"
                "Ejemplo: pip install pyarrow"
            ) from exc

    local_files.append(local_path)

print("Ficheros generados:")
for file_path in local_files:
    print(f"- {file_path}")

if args.dry_run:
    print("Dry-run activo: no se subieron ficheros a S3.")
    raise SystemExit(0)

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception as exc:
    raise SystemExit(
        "Falta boto3/botocore en el entorno. Hay que instalar dependencias y reintentarlo.\n"
        "Ejemplo: pip install boto3"
    ) from exc

s3_endpoint = os.getenv("S3_ENDPOINT_URL", os.getenv("S3_ENDPOINT", "http://localhost:9000"))
s3_region = os.getenv("AWS_REGION", os.getenv("S3_REGION", "us-east-1"))
s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio")))
s3_secret_key = os.getenv(
    "AWS_SECRET_ACCESS_KEY",
    os.getenv("S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123")),
)

s3_client = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    region_name=s3_region,
    aws_access_key_id=s3_access_key,
    aws_secret_access_key=s3_secret_key,
)

try:
    s3_client.head_bucket(Bucket=args.bucket)
except ClientError:
    s3_client.create_bucket(Bucket=args.bucket)

print("Subiendo a S3 ...")
uploaded = 0
for file_path in local_files:
    relative_path = file_path.relative_to(output_root).as_posix()
    s3_key = relative_path
    s3_client.upload_file(str(file_path), args.bucket, s3_key)
    uploaded += 1
    print(f"- s3://{args.bucket}/{s3_key}")

print(f"Subida completada. Archivos subidos: {uploaded}")
