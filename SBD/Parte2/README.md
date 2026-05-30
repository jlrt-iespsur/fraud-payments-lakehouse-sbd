# Parte 2 - Kafka -> Bronze (Iceberg)

Script:
- `SBD/Parte2/bronze_kafka_to_iceberg.py`
- `SBD/Parte2/run_bronze_parte2.sh`

Funcionalidad:
1. Lee eventos de pagos desde Kafka (`topic` configurable).
2. Parsea el JSON de cada mensaje al esquema Bronze.
3. Inserta en tabla Iceberg de capa Bronze con `ingestion_ts`.

Cómo ejecutarlo (streaming continuo):
```bash
./SBD/Parte2/run_bronze_parte2.sh \
  --bootstrap-servers kafka:9092 \
  --topic payments \
  --catalog lakehouse \
  --database payments \
  --table bronze_payments_parte2 \
  --checkpoint /opt/project/runtime/checkpoints/bronze_parte2
```

Uso para validación/lote (termina solo):
```bash
./SBD/Parte2/run_bronze_parte2.sh \
  --topic payments \
  --table bronze_payments_parte2_test \
  --checkpoint /opt/project/runtime/checkpoints/bronze_parte2_test \
  --starting-offsets earliest \
  --trigger available-now
```

Variables/flags útiles:
- `--starting-offsets`: `earliest` o `latest`.
- `--trigger`: `processing-time` (continuo) o `available-now` (batch sobre offsets disponibles).
- `--processing-interval-seconds`: intervalo del micro-batch en modo continuo.
- `--await-timeout-seconds`: timeout opcional para detener ejecución controlada.

Nota:
- El checkpoint debe ser estable para continuidad de offsets.
- Si cambias tabla/estrategia de lectura, usa checkpoint nuevo.
