#!/usr/bin/env python3

# PARTE 9 - S3 (Silver) -> RDS MySQL

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


parser = argparse.ArgumentParser(description="Lee Silver desde S3/MinIO y carga en MySQL RDS")
parser.add_argument(
    "--source-path",
    default=os.getenv(
        "SILVER_S3_PATH",
        "s3a://lakehouse/warehouse/payments/silver_payments/data",
    ),
)
parser.add_argument("--target-table", default=os.getenv("MYSQL_TARGET_TABLE", "silver_features"))
parser.add_argument("--target-database", default=os.getenv("MYSQL_DATABASE", "fraud"))
parser.add_argument("--mode", choices=["append", "overwrite"], default=os.getenv("MYSQL_WRITE_MODE", "append"))
parser.add_argument(
    "--jdbc-url",
    default=os.getenv(
        "MYSQL_JDBC_URL",
        "jdbc:mysql://mysql:3306/fraud?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
    ),
)
parser.add_argument("--jdbc-user", default=os.getenv("MYSQL_USER", "admin"))
parser.add_argument("--jdbc-password", default=os.getenv("MYSQL_PASSWORD", "admin"))
args = parser.parse_args()

s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
s3_access_key = os.getenv("S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio"))
s3_secret_key = os.getenv("S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
s3_region = os.getenv("AWS_REGION", os.getenv("S3_REGION", "us-east-1"))
s3_path_style = os.getenv("S3_PATH_STYLE_ACCESS", "true").lower()

os.environ.setdefault("AWS_REGION", s3_region)
os.environ.setdefault("AWS_DEFAULT_REGION", s3_region)

spark = (
    SparkSession.builder.appName("parte9-silver-s3-to-rds-mysql")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", s3_path_style)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false" if s3_endpoint.startswith("http://") else "true")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"Leyendo Silver desde: {args.source_path}")
silver = spark.read.parquet(args.source_path)

selected = silver.select(
    to_timestamp(col("event_time")).alias("event_time"),
    col("payment_id").cast("string").alias("payment_id"),
    col("customer_id").cast("string").alias("customer_id"),
    col("card_id").cast("string").alias("card_id"),
    col("merchant_id").cast("string").alias("merchant_id"),
    col("device_id").cast("string").alias("device_id"),
    col("ip").cast("string").alias("ip"),
    col("country").cast("string").alias("country"),
    col("amount").cast("double").alias("amount"),
    col("currency").cast("string").alias("currency"),
    col("status").cast("string").alias("status"),
    col("mcc").cast("string").alias("mcc"),
    col("tx_by_card_5m").cast("int").alias("tx_by_card_5m"),
    col("distinct_merchants_10m").cast("int").alias("distinct_merchants_10m"),
    col("distinct_countries_1h").cast("int").alias("distinct_countries_1h"),
    col("distinct_cards_per_device").cast("int").alias("distinct_cards_per_device"),
    col("declined_ratio_1h").cast("double").alias("declined_ratio_1h"),
).dropna(subset=["payment_id"])

full_table = f"{args.target_database}.{args.target_table}"

print(f"Escribiendo en MySQL: {full_table} (mode={args.mode})")
(
    selected.write.format("jdbc")
    .option("url", args.jdbc_url)
    .option("dbtable", full_table)
    .option("user", args.jdbc_user)
    .option("password", args.jdbc_password)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .mode(args.mode)
    .save()
)

print("Carga a MySQL completada")
spark.stop()
