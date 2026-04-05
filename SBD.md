# SBD - Bitácora rápida

Aquí voy dejando lo nuevo que hacemos en esta tarea, que parte del proyecto de Carlos.

## 2026-04-05 - Parte 1 (Generador Kafka)

Archivos nuevos:
- `SBD/Parte1/kafka_payment_generator.py`
- `SBD/Parte1/run_kafka_generator.sh`

Estos ficheros realizan las siguientes acciones:
- genera pagos de prueba y los manda a Kafka (`payments`)
- mete casos normales, reintentos y sospechosos
- saca los campos pedidos: `event_time`, `payment_id`, `customer_id`, `card_id`, `merchant_id`, `device_id`, `ip`, `country`, `amount`, `currency`, `status`, `mcc`

Ejecución:
```bash
./SBD/Parte1/run_kafka_generator.sh --events 5000 --sleep-ms 120
```

Nota rápida:
- se refactorizó para dejarlo en script secuencial (sin clases y sin `def`)

## 2026-04-05 - Parte 2 (Bronze)

Archivos creados:
- `SBD/Parte2/bronze_kafka_to_iceberg.py`
- `SBD/Parte2/run_bronze_parte2.sh`

Acciones:
- lee Kafka con Spark Streaming
- guarda en Iceberg capa Bronze
- mantiene el esquema original, sin lógica rara

Ejecución:
```bash
./SBD/Parte2/run_bronze_parte2.sh
```

Nota:
- por defecto escribe en la tabla `bronze_payments_parte2`
- para indicar otra tabla podemos usar: `--table nombre_tabla`
- refactorizado a script secuencial (sin clases y sin `def`)

## 2026-04-05 - Parte 3 (Silver)

Archivos creados:
- `SBD/Parte3/silver_bronze_enrichment.py`
- `SBD/Parte3/run_silver_parte3.sh`

Acciones:
- lee Bronze
- tipa datos (por ejemplo `event_time` a timestamp y `amount` a double)
- elimina duplicados por `payment_id`
- añade variables por ventanas de tiempo:
  - transacciones por tarjeta en 5 min
  - comercios distintos en 10 min
  - países distintos en 1h
  - tarjetas distintas por dispositivo
  - ratio de rechazadas en 1h
- guarda resultado en capa Silver

Ejecución:
```bash
./SBD/Parte3/run_silver_parte3.sh
```

Nota:
- por defecto lee de `bronze_payments_parte2`
- por defecto escribe en `silver_payments_parte3`
- para cambiar tablas: `--source-table` y `--target-table`
- refactorizado a script secuencial (sin clases y sin `def`)

## 2026-04-05 - Parte 4 (Gold)

Archivos creados:
- `SBD/Parte4/gold_fraud_detection.py`
- `SBD/Parte4/run_gold_parte4.sh`

Acciones:
- lee Silver
- aplica reglas de fraude
- calcula `risk_score`
- genera `reasons` (lista) y `reasons_text`
- genera tabla `fraud_alerts` (solo alertas)
- genera tabla tabular para análisis relacional

Ejecución:
```bash
./SBD/Parte4/run_gold_parte4.sh
```

Nota:
- por defecto lee de `payments.silver_payments_parte3`
- por defecto escribe en base `gold`
- tablas por defecto:
  - `gold.fraud_alerts_parte4`
  - `gold.payments_relations_parte4`
- script secuencial (sin clases y sin `def`)

## 2026-04-05 - Parte 5 (Consulta y visualizacion)

Archivos creados:
- `SBD/Parte5/check_trino_parte5.sh`
- `SBD/Parte5/dashboard_queries_parte5.sql`
- `SBD/Parte5/superset_setup_parte5.md`

Acciones:
- Trino ya conectado a Iceberg (catalogo `iceberg`)
- verificacion real de consultas sobre silver y gold
- queries listas para dashboard en Superset
- guia corta para conectar Superset -> Trino y montar el panel

Comprobacion hecha en Trino:
- total transacciones: `7299`
- total alertas: `7049`

Ejecucion:
```bash
./SBD/Parte5/check_trino_parte5.sh
```

## 2026-04-05 - Ajuste por copia de carpeta

Para no mezclar esta copia con otros proyectos:
- `docker-compose.yml` ahora usa nombre de proyecto `fraud-lakehouse-sbd`
- se quitaron todos los `container_name` fijos
- se reinició el stack completo

Resultado:
- los contenedores de esta carpeta quedan aislados
- `spark` monta correctamente este path:
  - `/Users/jlrtutor/CE_IA_y_BigData/BDA/ProyectoT2-SBD`
