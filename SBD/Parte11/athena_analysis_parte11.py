#!/usr/bin/env python3

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, date_trunc, lit, max as spark_max, sum as spark_sum, when


parser = argparse.ArgumentParser(description="Parte11 - Consultas y analisis (Athena-like) sobre S3")
parser.add_argument(
    "--input-path",
    default=os.getenv("PARTE11_INPUT_PATH", os.getenv("PARTE10_OUTPUT_PATH", "s3a://lakehouse/analytics/parte10/high_risk_payments")),
)
parser.add_argument(
    "--output-path",
    default=os.getenv("PARTE11_OUTPUT_PATH", "s3a://lakehouse/analytics/parte11/results"),
)
parser.add_argument("--output-format", choices=["parquet", "csv"], default=os.getenv("PARTE11_OUTPUT_FORMAT", "parquet"))
parser.add_argument("--output-mode", choices=["overwrite", "append"], default=os.getenv("PARTE11_OUTPUT_MODE", "overwrite"))
parser.add_argument("--high-risk-threshold", type=float, default=float(os.getenv("PARTE11_HIGH_RISK_THRESHOLD", "70")))
parser.add_argument("--min-high-risk-tx", type=int, default=int(os.getenv("PARTE11_MIN_HIGH_RISK_TX", "2")))
parser.add_argument("--tx5m-threshold", type=int, default=int(os.getenv("PARTE11_TX5M_THRESHOLD", "8")))
parser.add_argument("--countries-threshold", type=int, default=int(os.getenv("PARTE11_COUNTRIES_THRESHOLD", "2")))
parser.add_argument(
    "--geo-countries1h-threshold",
    type=int,
    default=int(os.getenv("PARTE11_GEO_COUNTRIES1H_THRESHOLD", os.getenv("PARTE11_COUNTRIES_THRESHOLD", "2"))),
)
parser.add_argument("--suspicious-countries", default=os.getenv("PARTE11_SUSPICIOUS_COUNTRIES", "RU,NG,KP,IR"))
parser.add_argument(
    "--compute-counts",
    action="store_true",
    default=os.getenv("PARTE11_COMPUTE_COUNTS", "false").lower() == "true",
)
args = parser.parse_args()

s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
s3_access_key = os.getenv("S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio"))
s3_secret_key = os.getenv("S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
s3_region = os.getenv("AWS_REGION", os.getenv("S3_REGION", "us-east-1"))
s3_path_style = os.getenv("S3_PATH_STYLE_ACCESS", "true").lower()

spark = (
    SparkSession.builder.appName("parte11-athena-analysis")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", os.getenv("PARTE11_SHUFFLE_PARTITIONS", "8"))
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


def write_result(df, subpath: str):
    out = f"{args.output_path.rstrip('/')}/{subpath}"
    writer = df.write.mode(args.output_mode)
    if args.output_format == "parquet":
        writer.parquet(out)
    else:
        writer.option("header", "true").csv(out)
    return out


print(f"Leyendo dataset consolidado de Parte10: {args.input_path}")
base = (
    spark.read.parquet(args.input_path)
    .withColumn("risk_score", col("risk_score").cast("double"))
    .withColumn("tx_5m", col("tx_5m").cast("int"))
    .withColumn("merchants_10m", col("merchants_10m").cast("int"))
    .withColumn("countries_1h", col("countries_1h").cast("int"))
)

suspicious_countries = [c.strip().upper() for c in args.suspicious_countries.split(",") if c.strip()]

# 1) Detectar transacciones de alto riesgo
high_risk_tx = (
    base.filter((col("risk_score") >= lit(args.high_risk_threshold)) | (col("status") == lit("declined")))
    .withColumn(
        "risk_reason",
        when(col("risk_score") >= lit(args.high_risk_threshold), lit("high_risk_score")).otherwise(lit("declined_status")),
    )
    .select(
        "payment_id",
        "card_id",
        "amount",
        "event_time",
        "event_date",
        "country",
        "status",
        "risk_score",
        "tx_5m",
        "merchants_10m",
        "countries_1h",
        "risk_reason",
    )
)

# 2) Identificar tarjetas con actividad sospechosa
suspicious_cards = (
    base.groupBy("card_id")
    .agg(
        count("*").alias("total_tx"),
        spark_sum(when(col("risk_score") >= lit(args.high_risk_threshold), lit(1)).otherwise(lit(0))).alias("high_risk_tx"),
        avg("risk_score").alias("avg_risk_score"),
        spark_max("tx_5m").alias("max_tx_5m"),
        spark_max("merchants_10m").alias("max_merchants_10m"),
        countDistinct("country").alias("distinct_countries"),
    )
    .filter(
        (col("high_risk_tx") >= lit(args.min_high_risk_tx))
        | (col("max_tx_5m") >= lit(args.tx5m_threshold))
        | (col("distinct_countries") >= lit(args.countries_threshold))
    )
)

# 3) Analizar patrones geográficos de fraude
geo_patterns = (
    base.withColumn("hour_bucket", date_trunc("hour", col("event_time")))
    .groupBy("country", "hour_bucket")
    .agg(
        count("*").alias("total_tx"),
        spark_sum(when(col("risk_score") >= lit(args.high_risk_threshold), lit(1)).otherwise(lit(0))).alias("high_risk_tx"),
        spark_sum(when(col("status") == lit("declined"), lit(1)).otherwise(lit(0))).alias("declined_tx"),
        avg("risk_score").alias("avg_risk_score"),
        spark_max("countries_1h").alias("max_countries_1h"),
        countDistinct("card_id").alias("distinct_cards"),
    )
    .withColumn("is_suspicious_country", when(col("country").isin(suspicious_countries), lit(1)).otherwise(lit(0)))
    .filter(
        (col("high_risk_tx") > lit(0))
        | (col("declined_tx") > lit(0))
        | (col("max_countries_1h") >= lit(args.geo_countries1h_threshold))
        | (col("is_suspicious_country") == lit(1))
    )
)

out_high = write_result(high_risk_tx, "high_risk_transactions")
out_cards = write_result(suspicious_cards, "suspicious_cards")
out_geo = write_result(geo_patterns, "geo_fraud_patterns")

summary = spark.createDataFrame(
    [
        ("high_risk_transactions", out_high),
        ("suspicious_cards", out_cards),
        ("geo_fraud_patterns", out_geo),
    ],
    ["dataset", "s3_output_path"],
)
out_summary = write_result(summary, "summary")

if args.compute_counts:
    print(f"Filas high_risk_transactions: {high_risk_tx.count()}")
    print(f"Filas suspicious_cards: {suspicious_cards.count()}")
    print(f"Filas geo_fraud_patterns: {geo_patterns.count()}")
print(f"Resumen de salidas: {out_summary}")
print("Parte11 completada")

spark.stop()
