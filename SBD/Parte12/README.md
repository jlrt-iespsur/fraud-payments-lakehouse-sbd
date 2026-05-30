# Parte 12 - Fine-tuning con Hugging Face

Script:
- `SBD/Parte12/fine_tuning_hf_parte12.py`
- `SBD/Parte12/run_parte12.sh`

Funcionalidad:
1. Descarga datasets consolidados desde S3/MinIO (ruta de Parte10 por defecto).
2. Limpia y prepara datos para entrenamiento.
3. Construye etiqueta de fraude (`label`) si no existe.
4. Hace fine-tuning de un modelo preentrenado de Hugging Face (`prajjwal1/bert-tiny` por defecto en local).
5. Evalúa (`accuracy`, `precision`, `recall`, `f1`, `roc_auc` cuando aplica).
6. Guarda modelo y métricas en local, con opción de publicar en Hugging Face Hub.

Cómo ejecutarlo (local):
```bash
./SBD/Parte12/run_parte12.sh \
  --s3-bucket lakehouse \
  --s3-prefix analytics/parte10/high_risk_payments \
  --model-name prajjwal1/bert-tiny \
  --epochs 2 \
  --batch-size 16
```

Modelo más grande (si tienes más memoria):
```bash
./SBD/Parte12/run_parte12.sh --model-name distilbert-base-uncased
```

Publicar en Hugging Face Hub:
```bash
export HF_TOKEN=<tu_token>
./SBD/Parte12/run_parte12.sh \
  --push-to-hub \
  --hub-model-id <usuario>/fraud-detector-parte12
```

Variables útiles:
- `S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `MINIO_BUCKET`
- `PARTE12_MODEL_NAME`
- `PARTE12_OUTPUT_DIR`
- `PARTE12_EPOCHS`, `PARTE12_BATCH_SIZE`, `PARTE12_LEARNING_RATE`
- `HF_TOKEN`, `PARTE12_HUB_MODEL_ID`

Nota:
- Si el dataset tiene solo una clase (solo fraude o solo no fraude), el script falla de forma explícita.
- En local se usa MinIO como capa S3 transparente.
