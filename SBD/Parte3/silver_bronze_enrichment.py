#!/usr/bin/env python3

# Spark batch Bronze -> Silver

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    array_distinct,
    avg,
    col,
    collect_list,
    row_number,
    round,
    size,
    to_timestamp,
    when,
)


# Parámetros aceptados desde el CLI
parser = argparse.ArgumentParser(description="Bronze -> Silver (limpieza y enriquecimiento)")
parser.add_argument("--catalog", default=os.getenv("ICEBERG_CATALOG", "lakehouse"))
parser.add_argument("--database", default=os.getenv("ICEBERG_DATABASE", "payments"))
parser.add_argument("--source-table", default="bronze_payments_parte2")
parser.add_argument("--target-table", default="silver_payments_parte3")
args = parser.parse_args()


# Configuración Spark + Iceberg
warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://lakehouse/warehouse")
jdbc_uri = os.getenv("ICEBERG_JDBC_URI", "jdbc:postgresql://postgres:5432/platform")
jdbc_user = os.getenv("ICEBERG_JDBC_USER", "lakehouse")
jdbc_password = os.getenv("ICEBERG_JDBC_PASSWORD", "lakehouse")
s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
s3_access_key = os.getenv("S3_ACCESS_KEY", "minio")
s3_secret_key = os.getenv("S3_SECRET_KEY", "minio123")
s3_region = os.getenv("AWS_REGION", os.getenv("S3_REGION", "us-east-1"))

os.environ.setdefault("AWS_REGION", s3_region)
os.environ.setdefault("AWS_DEFAULT_REGION", s3_region)

spark = (
    SparkSession.builder.appName("silver-bronze-enrichment-parte3")
    .config(f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{args.catalog}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
    .config(f"spark.sql.catalog.{args.catalog}.uri", jdbc_uri)
    .config(f"spark.sql.catalog.{args.catalog}.warehouse", warehouse)
    .config(f"spark.sql.catalog.{args.catalog}.jdbc.user", jdbc_user)
    .config(f"spark.sql.catalog.{args.catalog}.jdbc.password", jdbc_password)
    .config(f"spark.sql.catalog.{args.catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{args.catalog}.s3.endpoint", s3_endpoint)
    .config(f"spark.sql.catalog.{args.catalog}.s3.path-style-access", "true")
    .config(f"spark.sql.catalog.{args.catalog}.s3.access-key-id", s3_access_key)
    .config(f"spark.sql.catalog.{args.catalog}.s3.secret-access-key", s3_secret_key)
    .config(f"spark.sql.catalog.{args.catalog}.s3.region", s3_region)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# Origen y destino
source = f"{args.catalog}.{args.database}.{args.source_table}"
target = f"{args.catalog}.{args.database}.{args.target_table}"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.database}")
bronze = spark.table(source)

# Tareas a realizar sobre los datos al pasar de una tabla a otra
# 1) Tipado y limpieza básica
typed = (
    bronze.withColumn("event_time_ts", to_timestamp("event_time"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("status", col("status").cast("string"))
    .withColumn("mcc", col("mcc").cast("string"))
    .filter(col("event_time_ts").isNotNull())
    .filter(col("payment_id").isNotNull())
    .filter(col("card_id").isNotNull())
    .filter(col("device_id").isNotNull())
)

# 2) Deduplicación
dedup_window = Window.partitionBy("payment_id").orderBy(col("event_time_ts").desc())
deduped = typed.withColumn("row_num", row_number().over(dedup_window)).filter(col("row_num") == 1).drop("row_num")

# 3) Variables por ventanas temporales
# esto es importante porque vamos a fijar unos intervalos de tiempo en los que
# vamos a ver si una tarjeta tuvo actividad o no (y dónde), o qué hizo exactamente
# los últimos 5min, 10min y 1hora...
event_epoch = col("event_time_ts").cast("long")
card_window_5m = Window.partitionBy("card_id").orderBy(event_epoch).rangeBetween(-300, 0)
card_window_10m = Window.partitionBy("card_id").orderBy(event_epoch).rangeBetween(-600, 0)
card_window_1h = Window.partitionBy("card_id").orderBy(event_epoch).rangeBetween(-3600, 0)
device_window = Window.partitionBy("device_id")

silver = (
    deduped.withColumn("tx_by_card_5m", size(collect_list("payment_id").over(card_window_5m)))
    .withColumn(
        "distinct_merchants_10m",
        size(array_distinct(collect_list("merchant_id").over(card_window_10m))),
    )
    .withColumn(
        "distinct_countries_1h",
        size(array_distinct(collect_list("country").over(card_window_1h))),
    )
    .withColumn(
        "distinct_cards_per_device",
        size(array_distinct(collect_list("card_id").over(device_window))),
    )
    .withColumn(
        "declined_ratio_1h",
        round(
            avg(when(col("status") == "declined", 1.0).otherwise(0.0)).over(card_window_1h),
            4,
        ),
    )
    .select(
        col("event_time_ts").alias("event_time"),
        "payment_id",
        "customer_id",
        "card_id",
        "merchant_id",
        "device_id",
        "ip",
        "country",
        "amount",
        "currency",
        "status",
        "mcc",
        "tx_by_card_5m",
        "distinct_merchants_10m",
        "distinct_countries_1h",
        "distinct_cards_per_device",
        "declined_ratio_1h",
    )
)

# Escribimos en Silver
silver.writeTo(target).using("iceberg").createOrReplace()
spark.stop()
