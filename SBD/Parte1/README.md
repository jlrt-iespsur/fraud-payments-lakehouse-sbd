# Parte 1 - Generador de pagos en Kafka

Script:
- `SBD/Parte1/kafka_payment_generator.py`
- `SBD/Parte1/run_kafka_generator.sh`

Funcionalidad:
1. Genera eventos sintéticos de pago para el topic Kafka `payments`.
2. Simula tres escenarios: `normal`, `retry` y `suspicious`.
3. Publica eventos con los campos base del pipeline (Bronze/Silver/Gold).

Cómo ejecutarlo:
```bash
./SBD/Parte1/run_kafka_generator.sh \
  --bootstrap-servers kafka:9092 \
  --topic payments \
  --events 5000 \
  --sleep-ms 120 \
  --profiles 250 \
  --seed 42
```

Uso rápido:
```bash
./SBD/Parte1/run_kafka_generator.sh --events 1000 --sleep-ms 80
```

Variables/flags útiles:
- `--bootstrap-servers`: broker Kafka destino.
- `--topic`: topic Kafka.
- `--events`: número total de eventos.
- `--sleep-ms`: pausa entre eventos (simula ritmo de llegada).
- `--profiles`: número de perfiles cliente/tarjeta.
- `--seed`: semilla para reproducibilidad.

Nota:
- Si `--sleep-ms 0`, envía a máxima velocidad.
- El script envía con `acks=all` para priorizar fiabilidad.
