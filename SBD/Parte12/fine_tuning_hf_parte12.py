#!/usr/bin/env python3

import argparse
import json
import os
from io import BytesIO
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
from datasets import Dataset
from huggingface_hub import login
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    DataCollatorWithPadding,
    Trainer,
    TrainingArguments,
)


def parse_args():
    p = argparse.ArgumentParser(description="Parte12 - Fine-tuning HF para fraude")
    p.add_argument("--s3-bucket", default=os.getenv("MINIO_BUCKET", "lakehouse"))
    p.add_argument("--s3-prefix", default=os.getenv("PARTE10_OUTPUT_PREFIX", "analytics/parte10/high_risk_payments"))
    p.add_argument(
        "--s3-endpoint",
        default=os.getenv("S3_ENDPOINT", os.getenv("S3_ENDPOINT_URL", "http://minio:9000")),
    )
    p.add_argument("--s3-access-key", default=os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minio")))
    p.add_argument("--s3-secret-key", default=os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123")))
    p.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    p.add_argument("--model-name", default=os.getenv("PARTE12_MODEL_NAME", "prajjwal1/bert-tiny"))
    p.add_argument("--output-dir", default=os.getenv("PARTE12_OUTPUT_DIR", "runtime/models/parte12-fraud-hf"))
    p.add_argument("--max-files", type=int, default=int(os.getenv("PARTE12_MAX_FILES", "200")))
    p.add_argument("--max-rows", type=int, default=int(os.getenv("PARTE12_MAX_ROWS", "50000")))
    p.add_argument("--test-size", type=float, default=float(os.getenv("PARTE12_TEST_SIZE", "0.2")))
    p.add_argument("--random-state", type=int, default=int(os.getenv("PARTE12_RANDOM_STATE", "42")))
    p.add_argument("--epochs", type=int, default=int(os.getenv("PARTE12_EPOCHS", "2")))
    p.add_argument("--batch-size", type=int, default=int(os.getenv("PARTE12_BATCH_SIZE", "16")))
    p.add_argument("--lr", type=float, default=float(os.getenv("PARTE12_LEARNING_RATE", "2e-5")))
    p.add_argument("--max-length", type=int, default=int(os.getenv("PARTE12_MAX_LENGTH", "128")))
    p.add_argument("--high-risk-threshold", type=float, default=float(os.getenv("PARTE12_HIGH_RISK_THRESHOLD", "70")))
    p.add_argument("--push-to-hub", action="store_true")
    p.add_argument("--hub-model-id", default=os.getenv("PARTE12_HUB_MODEL_ID", ""))
    p.add_argument("--hub-token", default=os.getenv("HF_TOKEN", ""))
    return p.parse_args()


def s3_client(args):
    return boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_access_key,
        aws_secret_access_key=args.s3_secret_key,
        region_name=args.region,
    )


def list_parquet_keys(client, bucket: str, prefix: str, max_files: int):
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
                if len(keys) >= max_files:
                    return keys
    return keys


def read_parquet_from_s3(client, bucket: str, keys: list[str], max_rows: int) -> pd.DataFrame:
    chunks = []
    total = 0
    for key in keys:
        raw = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        df = pd.read_parquet(BytesIO(raw))
        chunks.append(df)
        total += len(df)
        if total >= max_rows:
            break
    if not chunks:
        return pd.DataFrame()
    out = pd.concat(chunks, ignore_index=True)
    return out.head(max_rows)


def normalize_dataframe(df: pd.DataFrame, high_risk_threshold: float) -> pd.DataFrame:
    if df.empty:
        return df

    rename_map = {
        "tx_by_card_5m": "tx_5m",
        "distinct_merchants_10m": "merchants_10m",
        "distinct_countries_1h": "countries_1h",
    }
    df = df.rename(columns=rename_map)

    for col in ["payment_id", "card_id", "country", "status"]:
        if col not in df.columns:
            df[col] = ""

    for col in ["amount", "risk_score", "tx_5m", "merchants_10m", "countries_1h"]:
        if col not in df.columns:
            df[col] = 0

    df["payment_id"] = df["payment_id"].astype(str).str.strip()
    df["card_id"] = df["card_id"].astype(str).str.strip().str.upper()
    df["country"] = df["country"].astype(str).str.strip().str.upper().replace({"ESPAÑA": "ES", "SPAIN": "ES"})
    df["status"] = df["status"].astype(str).str.strip().str.lower()

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0)
    df["tx_5m"] = pd.to_numeric(df["tx_5m"], errors="coerce").fillna(0)
    df["merchants_10m"] = pd.to_numeric(df["merchants_10m"], errors="coerce").fillna(0)
    df["countries_1h"] = pd.to_numeric(df["countries_1h"], errors="coerce").fillna(0)

    df = df[df["payment_id"] != ""]
    df = df[df["card_id"] != ""]
    df = df[df["amount"].notna() & (df["amount"] >= 0)]
    df = df.drop_duplicates(subset=["payment_id"], keep="last")

    if "label" in df.columns:
        labels = pd.to_numeric(df["label"], errors="coerce").fillna(0).astype(int)
    elif "is_fraud" in df.columns:
        labels = pd.to_numeric(df["is_fraud"], errors="coerce").fillna(0).astype(int)
    else:
        labels = ((df["risk_score"] >= high_risk_threshold) | (df["status"] == "declined")).astype(int)

    df["label"] = labels
    return df


def to_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["text"] = (
        "amount="
        + df["amount"].round(2).astype(str)
        + " | card="
        + df["card_id"].astype(str)
        + " | country="
        + df["country"].astype(str)
        + " | status="
        + df["status"].astype(str)
        + " | risk_score="
        + df["risk_score"].round(2).astype(str)
        + " | tx_5m="
        + df["tx_5m"].astype(int).astype(str)
        + " | merchants_10m="
        + df["merchants_10m"].astype(int).astype(str)
        + " | countries_1h="
        + df["countries_1h"].astype(int).astype(str)
    )
    return df[["text", "label"]]


def compute_metrics(eval_pred):
    logits, labels = eval_pred
    probs = np.exp(logits) / np.exp(logits).sum(axis=-1, keepdims=True)
    preds = np.argmax(logits, axis=-1)
    out = {
        "accuracy": float(accuracy_score(labels, preds)),
        "precision": float(precision_score(labels, preds, zero_division=0)),
        "recall": float(recall_score(labels, preds, zero_division=0)),
        "f1": float(f1_score(labels, preds, zero_division=0)),
    }
    if len(set(labels.tolist())) > 1:
        out["roc_auc"] = float(roc_auc_score(labels, probs[:, 1]))
    return out


def main():
    args = parse_args()

    if args.push_to_hub and args.hub_token:
        login(token=args.hub_token)

    client = s3_client(args)
    keys = list_parquet_keys(client, args.s3_bucket, args.s3_prefix, args.max_files)
    if not keys:
        raise RuntimeError(f"No se encontraron ficheros parquet en s3://{args.s3_bucket}/{args.s3_prefix}")

    raw_df = read_parquet_from_s3(client, args.s3_bucket, keys, args.max_rows)
    data_df = normalize_dataframe(raw_df, args.high_risk_threshold)
    if data_df.empty:
        raise RuntimeError("Dataset vacío tras limpieza")

    class_counts = data_df["label"].value_counts().to_dict()
    if len(class_counts) < 2:
        raise RuntimeError("El dataset solo tiene una clase. Ajusta el origen para incluir fraude y no fraude")

    model_df = to_text(data_df)

    train_df, eval_df = train_test_split(
        model_df,
        test_size=args.test_size,
        random_state=args.random_state,
        stratify=model_df["label"],
    )

    train_ds = Dataset.from_pandas(train_df.reset_index(drop=True))
    eval_ds = Dataset.from_pandas(eval_df.reset_index(drop=True))

    tokenizer = AutoTokenizer.from_pretrained(args.model_name)

    def tokenize(batch):
        return tokenizer(batch["text"], truncation=True, max_length=args.max_length)

    train_ds = train_ds.map(tokenize, batched=True)
    eval_ds = eval_ds.map(tokenize, batched=True)
    train_ds = train_ds.rename_column("label", "labels")
    eval_ds = eval_ds.rename_column("label", "labels")
    train_ds.set_format(type="torch", columns=["input_ids", "attention_mask", "labels"])
    eval_ds.set_format(type="torch", columns=["input_ids", "attention_mask", "labels"])

    model = AutoModelForSequenceClassification.from_pretrained(args.model_name, num_labels=2)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    training_args = TrainingArguments(
        output_dir=str(output_dir),
        learning_rate=args.lr,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        num_train_epochs=args.epochs,
        weight_decay=0.01,
        eval_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="f1",
        report_to="none",
        push_to_hub=args.push_to_hub,
        hub_model_id=args.hub_model_id if args.push_to_hub else None,
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=eval_ds,
        tokenizer=tokenizer,
        data_collator=DataCollatorWithPadding(tokenizer=tokenizer),
        compute_metrics=compute_metrics,
    )

    trainer.train()
    eval_metrics = trainer.evaluate()

    trainer.save_model(str(output_dir / "model"))
    tokenizer.save_pretrained(str(output_dir / "model"))

    metrics = {
        "rows_total": int(len(model_df)),
        "train_rows": int(len(train_df)),
        "eval_rows": int(len(eval_df)),
        "class_distribution": {str(k): int(v) for k, v in class_counts.items()},
        "model_name": args.model_name,
        "eval_metrics": {k: float(v) for k, v in eval_metrics.items() if isinstance(v, (int, float))},
    }

    metrics_path = output_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    if args.push_to_hub:
        trainer.push_to_hub()

    print(json.dumps(metrics, indent=2))
    print(f"Modelo guardado en: {output_dir / 'model'}")
    print(f"Métricas guardadas en: {metrics_path}")


if __name__ == "__main__":
    main()
