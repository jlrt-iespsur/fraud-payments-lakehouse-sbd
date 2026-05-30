# Parte 3 - Bronze -> Silver (enriquecimiento)

Script:
- `SBD/Parte3/silver_bronze_enrichment.py`
- `SBD/Parte3/run_silver_parte3.sh`

Funcionalidad:
1. Lee datos desde Bronze (Iceberg).
2. Limpia, tipa y deduplica por `payment_id`.
3. Calcula features temporales (5m, 10m, 1h) para detección de fraude.
4. Escribe resultado en tabla Silver.

Cómo ejecutarlo:
```bash
./SBD/Parte3/run_silver_parte3.sh \
  --catalog lakehouse \
  --database payments \
  --source-table bronze_payments_parte2 \
  --target-table silver_payments_parte3
```

Uso rápido:
```bash
./SBD/Parte3/run_silver_parte3.sh
```

Variables/flags útiles:
- `--catalog`
- `--database`
- `--source-table`
- `--target-table`

Nota:
- Es un job batch: ejecuta y termina.
