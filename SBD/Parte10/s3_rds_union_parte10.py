#!/usr/bin/env python3

# PARTE 10 - ELT S3 + RDS MySQL

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    lower,
    regexp_replace,
    row_number,
    to_date,
    to_timestamp,
    trim,
    upper,
    when,
)


parser = argparse.ArgumentParser(description="Union y filtrado de dataset S3 + RDS")
parser.add_argument(
    "--s3-input-path",
    default=os.getenv("PARTE10_S3_INPUT_PATH", os.getenv("SILVER_S3_PATH", "s3a://lakehouse/warehouse/payments/silver_payments/data")),
)
parser.add_argument(
    "--mysql-jdbc-url",
    default=os.getenv("PARTE10_MYSQL_JDBC_URL", os.getenv("MYSQL_JDBC_URL", "jdbc:mysql://mysql:3306/fraud?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC")),
)
parser.add_argument("--mysql-user", default=os.getenv("PARTE10_MYSQL_USER", os.getenv("MYSQL_USER", "admin")))
parser.add_argument("--mysql-password", default=os.getenv("PARTE10_MYSQL_PASSWORD", os.getenv("MYSQL_PASSWORD", "admin")))
parser.add_argument("--mysql-database", default=os.getenv("PARTE10_MYSQL_DATABASE", os.getenv("MYSQL_DATABASE", "fraud")))
parser.add_argument("--mysql-table", default=os.getenv("PARTE10_MYSQL_TABLE", os.getenv("MYSQL_TARGET_TABLE", "silver_features")))
parser.add_argument(
    "--output-path",
    default=os.getenv("PARTE10_OUTPUT_PATH", "s3a://lakehouse/analytics/parte10/high_risk_payments"),
)
parser.add_argument("--output-mode", choices=["append", "overwrite"], default=os.getenv("PARTE10_OUTPUT_MODE", "overwrite"))
parser.add_argument("--high-risk-threshold", type=float, default=float(os.getenv("PARTE10_HIGH_RISK_THRESHOLD", "60")))
parser.add_argument("--tx5m-threshold", type=int, default=int(os.getenv("PARTE10_TX5M_THRESHOLD", "5")))
parser.add_argument("--merchants10m-threshold", type=int, default=int(os.getenv("PARTE10_MERCHANTS10M_THRESHOLD", "4")))
parser.add_argument("--countries1h-threshold", type=int, default=int(os.getenv("PARTE10_COUNTRIES1H_THRESHOLD", "3")))
parser.add_argument("--suspicious-countries", default=os.getenv("PARTE10_SUSPICIOUS_COUNTRIES", "RU,NG,KP,IR"))
parser.add_argument(
    "--compute-counts",
    action="store_true",
    default=os.getenv("PARTE10_COMPUTE_COUNTS", "false").lower() == "true",
    help="Calcula conteos finales completos (puede consumir más recursos).",
)
args = parser.parse_args()

s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
s3_access_key = os.getenv("S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio"))
s3_secret_key = os.getenv("S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
s3_region = os.getenv("AWS_REGION", os.getenv("S3_REGION", "us-east-1"))
s3_path_style = os.getenv("S3_PATH_STYLE_ACCESS", "true").lower()

os.environ.setdefault("AWS_REGION", s3_region)
os.environ.setdefault("AWS_DEFAULT_REGION", s3_region)

spark = (
    SparkSession.builder.appName("parte10-s3-rds-union")
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


def pick_column(df: DataFrame, options: list[str], cast_type: str | None = None):
    for name in options:
        if name in df.columns:
            c = col(name)
            return c.cast(cast_type) if cast_type else c
    return lit(None).cast(cast_type if cast_type else "string")


def normalize_country(country_col):
    c = lower(trim(country_col.cast("string")))
    c = regexp_replace(c, "á", "a")
    c = regexp_replace(c, "é", "e")
    c = regexp_replace(c, "í", "i")
    c = regexp_replace(c, "ó", "o")
    c = regexp_replace(c, "ú", "u")
    return (
        when(c.isin("es", "espana", "spain"), lit("ES"))
        .when(c.isin("pt", "portugal"), lit("PT"))
        .when(c.isin("fr", "france"), lit("FR"))
        .when(c.isin("it", "italy", "italia"), lit("IT"))
        .when(c.isin("de", "germany", "alemania"), lit("DE"))
        .when(c.isin("gb", "uk", "united kingdom", "reino unido"), lit("GB"))
        .when(c.isin("us", "usa", "united states", "estados unidos"), lit("US"))
        .when(c.isin("mx", "mexico"), lit("MX"))
        .otherwise(upper(trim(country_col.cast("string"))))
    )


def normalize_status(status_col):
    s = lower(trim(status_col.cast("string")))
    return (
        when(s.isin("approved", "approve", "ok", "success"), lit("approved"))
        .when(s.isin("declined", "deny", "denied", "rejected", "fail", "failed"), lit("declined"))
        .otherwise(s)
    )


def normalize_event_time(df: DataFrame):
    src = pick_column(df, ["event_time", "event_ts", "timestamp"], "string")
    cleaned = regexp_replace(src, "T", " ")
    cleaned = regexp_replace(cleaned, "Z", "")
    return coalesce(
        to_timestamp(src),
        to_timestamp(cleaned),
        to_timestamp(src, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        to_timestamp(src, "yyyy-MM-dd HH:mm:ss"),
    )


def canonicalize(df: DataFrame, source_name: str) -> DataFrame:
    base = (
        df.withColumn("payment_id", trim(pick_column(df, ["payment_id"], "string")))
        .withColumn("card_id", upper(trim(pick_column(df, ["card_id"], "string"))))
        .withColumn("amount", pick_column(df, ["amount", "amt"], "double"))
        .withColumn("event_time", normalize_event_time(df))
        .withColumn("country", normalize_country(pick_column(df, ["country", "country_code", "pais"], "string")))
        .withColumn("status", normalize_status(pick_column(df, ["status", "payment_status"], "string")))
        .withColumn("risk_score", coalesce(pick_column(df, ["risk_score"], "double"), lit(0.0)))
        .withColumn("tx_5m", coalesce(pick_column(df, ["tx_5m", "tx_by_card_5m"], "int"), lit(0)))
        .withColumn("merchants_10m", coalesce(pick_column(df, ["merchants_10m", "distinct_merchants_10m"], "int"), lit(0)))
        .withColumn("countries_1h", coalesce(pick_column(df, ["countries_1h", "distinct_countries_1h"], "int"), lit(0)))
        .withColumn("source", lit(source_name))
        .select(
            "payment_id",
            "card_id",
            "amount",
            "event_time",
            "country",
            "status",
            "risk_score",
            "tx_5m",
            "merchants_10m",
            "countries_1h",
            "source",
        )
    )

    # Registros críticos: payment_id, card_id y amount válidos.
    cleaned = (
        base.filter(col("payment_id").isNotNull() & (col("payment_id") != ""))
        .filter(col("card_id").isNotNull() & (col("card_id") != ""))
        .filter(col("amount").isNotNull() & (col("amount") >= 0.0))
        .filter(col("event_time").isNotNull())
    )

    # Eliminación de duplicados por payment_id, conservando la fila más reciente.
    w = Window.partitionBy("payment_id").orderBy(col("event_time").desc())
    return cleaned.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")


print(f"Leyendo S3: {args.s3_input_path}")
s3_df = spark.read.parquet(args.s3_input_path)
s3_clean = canonicalize(s3_df, "s3")

mysql_dbtable = f"{args.mysql_database}.{args.mysql_table}"
print(f"Leyendo RDS MySQL: {mysql_dbtable}")
rds_df = (
    spark.read.format("jdbc")
    .option("url", args.mysql_jdbc_url)
    .option("dbtable", mysql_dbtable)
    .option("user", args.mysql_user)
    .option("password", args.mysql_password)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
)
rds_clean = canonicalize(rds_df, "rds")

print("Uniendo datasets por payment_id")
joined = s3_clean.alias("s").join(rds_clean.alias("r"), on="payment_id", how="outer")

merged = (
    joined.select(
        col("payment_id"),
        coalesce(col("s.card_id"), col("r.card_id")).alias("card_id"),
        coalesce(col("s.amount"), col("r.amount")).alias("amount"),
        coalesce(col("s.event_time"), col("r.event_time")).alias("event_time"),
        coalesce(col("s.country"), col("r.country")).alias("country"),
        coalesce(col("s.status"), col("r.status")).alias("status"),
        coalesce(col("s.risk_score"), col("r.risk_score"), lit(0.0)).alias("risk_score"),
        coalesce(col("s.tx_5m"), col("r.tx_5m"), lit(0)).alias("tx_5m"),
        coalesce(col("s.merchants_10m"), col("r.merchants_10m"), lit(0)).alias("merchants_10m"),
        coalesce(col("s.countries_1h"), col("r.countries_1h"), lit(0)).alias("countries_1h"),
        col("s.source").alias("source_s3"),
        col("r.source").alias("source_rds"),
    )
    .filter(col("card_id").isNotNull() & (col("card_id") != ""))
    .filter(col("amount").isNotNull() & (col("amount") >= 0.0))
    .withColumn("event_date", to_date(col("event_time")))
)

suspicious_countries = [c.strip().upper() for c in args.suspicious_countries.split(",") if c.strip()]

high_risk_filter = col("risk_score") >= lit(args.high_risk_threshold)
freq_filter = (col("tx_5m") >= lit(args.tx5m_threshold)) | (col("merchants_10m") >= lit(args.merchants10m_threshold))
geo_filter = (col("countries_1h") >= lit(args.countries1h_threshold)) | col("country").isin(suspicious_countries)

filtered = merged.filter(high_risk_filter | freq_filter | geo_filter)

print(f"Guardando resultado en: {args.output_path} (mode={args.output_mode})")
filtered.write.mode(args.output_mode).partitionBy("event_date").parquet(args.output_path)

if args.compute_counts:
    print(f"Filas S3 limpias: {s3_clean.count()}")
    print(f"Filas RDS limpias: {rds_clean.count()}")
    print(f"Filas unificadas: {merged.count()}")
    print(f"Filas filtradas: {filtered.count()}")
else:
    print("Conteos finales omitidos (--compute-counts para habilitar).")
print("Parte10 completada")
spark.stop()
