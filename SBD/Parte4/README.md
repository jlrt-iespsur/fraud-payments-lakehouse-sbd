# Parte 4 - Silver -> Gold (fraude)

Script:
- `SBD/Parte4/gold_fraud_detection.py`
- `SBD/Parte4/run_gold_parte4.sh`

Funcionalidad:
1. Lee Silver enriquecido.
2. Aplica reglas de fraude y calcula `risk_score`.
3. Genera tabla de alertas (`fraud_alerts_parte4`).
4. Genera tabla relacional para análisis (`payments_relations_parte4`).

Cómo ejecutarlo:
```bash
./SBD/Parte4/run_gold_parte4.sh \
  --catalog lakehouse \
  --source-database payments \
  --source-table silver_payments_parte3 \
  --target-database gold \
  --alerts-table fraud_alerts_parte4 \
  --relations-table payments_relations_parte4
```

Uso rápido:
```bash
./SBD/Parte4/run_gold_parte4.sh
```

Variables/flags útiles:
- `--catalog`
- `--source-database`
- `--source-table`
- `--target-database`
- `--alerts-table`
- `--relations-table`

Nota:
- Es un job batch: recalcula Gold completo.
