#!/usr/bin/env python3

# Spark batch Silver -> Gold: detección de fraude y dataset relacional tabular

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, concat_ws, expr, lit, size, when



# parámetros válidos del CLI
parser = argparse.ArgumentParser(description="Silver -> Gold (fraud alerts + tabla relacional)")
parser.add_argument("--catalog", default=os.getenv("ICEBERG_CATALOG", "lakehouse"))
parser.add_argument("--source-database", default=os.getenv("ICEBERG_DATABASE", "payments"))
parser.add_argument("--source-table", default="silver_payments_parte3")
parser.add_argument("--target-database", default="gold")
parser.add_argument("--alerts-table", default="fraud_alerts_parte4")
parser.add_argument("--relations-table", default="payments_relations_parte4")
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
    SparkSession.builder.appName("gold-fraud-detection-parte4")
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



# Origen y destinos
source = f"{args.catalog}.{args.source_database}.{args.source_table}"
alerts_target = f"{args.catalog}.{args.target_database}.{args.alerts_table}"
relations_target = f"{args.catalog}.{args.target_database}.{args.relations_table}"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.target_database}")
silver = spark.table(source)


# Reglas de fraude
enriched = (
    silver.withColumn(
        "raw_reasons",
        array(
            when(col("tx_by_card_5m") >= 5, lit("high_card_velocity_5m")),
            when(col("distinct_merchants_10m") >= 4, lit("many_merchants_10m")),
            when(col("distinct_countries_1h") >= 3, lit("many_countries_1h")),
            when(col("distinct_cards_per_device") >= 4, lit("device_shared_by_cards")),
            when(col("declined_ratio_1h") >= 0.45, lit("high_declined_ratio_1h")),
            when(col("amount") >= 1200, lit("high_amount")),
        ),
    )
    .withColumn("reasons", expr("filter(raw_reasons, x -> x is not null)"))
    .withColumn(
        "risk_score",
        when(col("tx_by_card_5m") >= 5, 24).otherwise(0)
        + when(col("distinct_merchants_10m") >= 4, 18).otherwise(0)
        + when(col("distinct_countries_1h") >= 3, 18).otherwise(0)
        + when(col("distinct_cards_per_device") >= 4, 16).otherwise(0)
        + when(col("declined_ratio_1h") >= 0.45, 12).otherwise(0)
        + when(col("amount") >= 1200, 12).otherwise(0),
    )
    .withColumn("reasons_text", concat_ws(", ", col("reasons")))
    .withColumn("is_alert", size(col("reasons")) > 0)
    .drop("raw_reasons")
)

# Tabla Gold 1: fraude
fraud_alerts = (
    enriched.filter(col("is_alert"))
    .select(
        "payment_id",
        "event_time",
        "customer_id",
        "card_id",
        "merchant_id",
        "device_id",
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
        "risk_score",
        "reasons",
        "reasons_text",
    )
)

# Tabla Gold 2: modelo tabular relacional con los datos agregaods
payments_relations = enriched.select(
    "payment_id",
    "event_time",
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
    "risk_score",
    "reasons",
    "reasons_text",
    "is_alert",
)

# escribimos los datos en la tabla Gold
fraud_alerts.writeTo(alerts_target).using("iceberg").createOrReplace()
payments_relations.writeTo(relations_target).using("iceberg").createOrReplace()
spark.stop()
