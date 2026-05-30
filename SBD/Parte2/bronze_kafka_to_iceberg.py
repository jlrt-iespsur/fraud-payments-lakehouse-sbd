#!/usr/bin/env python3

# Spark Structured Streaming: Kafka -> Iceberg (Bronze)

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


# Estructura base del evento
payment_event_schema = StructType(
    [
        StructField("event_time", StringType(), False),
        StructField("payment_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("card_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("ip", StringType(), False),
        StructField("country", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("status", StringType(), False),
        StructField("mcc", StringType(), False),
    ]
)


# Parámetros de configuración desde el CLI
parser = argparse.ArgumentParser(description="Kafka -> Iceberg Bronze (datos crudos)")
parser.add_argument("--bootstrap-servers", default="kafka:9092")
parser.add_argument("--topic", default="payments")
parser.add_argument("--catalog", default=os.getenv("ICEBERG_CATALOG", "lakehouse"))
parser.add_argument("--database", default=os.getenv("ICEBERG_DATABASE", "payments"))
parser.add_argument("--table", default="bronze_payments_parte2")
parser.add_argument("--checkpoint", default="/opt/project/runtime/checkpoints/bronze_parte2")
parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
parser.add_argument("--trigger", choices=["processing-time", "available-now"], default="processing-time")
parser.add_argument("--processing-interval-seconds", type=int, default=5)
parser.add_argument("--await-timeout-seconds", type=int, default=0)
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
    SparkSession.builder.appName("bronze-kafka-to-iceberg-parte2")
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


# Tabla Bronze destino
full_table_name = f"{args.catalog}.{args.database}.{args.table}"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {args.catalog}.{args.database}")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        event_time STRING,
        payment_id STRING,
        customer_id STRING,
        card_id STRING,
        merchant_id STRING,
        device_id STRING,
        ip STRING,
        country STRING,
        amount DOUBLE,
        currency STRING,
        status STRING,
        mcc STRING,
        ingestion_ts TIMESTAMP
    )
    USING iceberg
    """
)


# Lectura Kafka y escritura Bronze
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", args.bootstrap_servers)
    .option("subscribe", args.topic)
    .option("startingOffsets", args.starting_offsets)
    .option("failOnDataLoss", "false")
    .load()
)

# Parseo JSON de Kafka y descarte de mensajes corruptos (payload nulo).
bronze_df = (
    kafka_stream.select(from_json(col("value").cast("string"), payment_event_schema).alias("payload"))
    .select("payload.*")
    .filter(col("payment_id").isNotNull())
    .withColumn("ingestion_ts", current_timestamp())
)

# Definimos el writer una sola vez y solo variamos el trigger según el modo.
writer = (
    bronze_df.writeStream.format("iceberg")
    .outputMode("append")
    .queryName("bronze_streaming_parte2")
    .option("checkpointLocation", args.checkpoint)
)

if args.trigger == "available-now":
    query = writer.trigger(availableNow=True).toTable(full_table_name)
else:
    query = writer.trigger(processingTime=f"{args.processing_interval_seconds} seconds").toTable(full_table_name)

print(f"Bronze Parte2 iniciada en {full_table_name}: id={query.id}, run_id={query.runId}", flush=True)

# Si se indica timeout, permitimos ejecución controlada y cierre limpio.
if args.await_timeout_seconds > 0:
    query.awaitTermination(args.await_timeout_seconds)
    if query.isActive:
        query.stop()
else:
    query.awaitTermination()

# controlamos posibles errores al finalizar
exception = query.exception()
if exception is not None:
    raise RuntimeError(f"La query Bronze Parte2 se ha detenido con error: {exception}")

spark.stop()
