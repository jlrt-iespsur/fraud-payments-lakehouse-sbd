# Superset + Trino (Parte 5)

## 1) Levantar servicios

```bash
docker compose up -d trino superset
```

Opcional (script Python):
```bash
python3 SBD/Parte5/setup_superset_trino_parte5.py
```

Superset queda en:
- `http://localhost:8088`

## 2) Conectar Superset a Trino

En Superset:
1. `Settings` -> `Database Connections` -> `+ Database`
2. Selecciona `Trino`
3. SQLAlchemy URI:

```text
trino://trino@host.docker.internal:8080/iceberg/payments
```

4. Guarda y pulsa `Test Connection`

Si quieres configurarlo por script:
```bash
python3 SBD/Parte5/setup_superset_trino_parte5.py \
  --db-name trino_iceberg \
  --trino-uri trino://trino@trino:8080/iceberg/payments
```

## 3) Dataset principal

Crear datasets desde:
- `iceberg.payments.silver_payments`
- `iceberg.payments.fraud_alerts`

## 4) Dashboard recomendado

Usar las consultas de:
- `SBD/Parte5/dashboard_queries_parte5.sql`

Gráficos mínimos:
- Big Number: total transacciones
- Big Number: total alertas fraude
- Time-series line/bar: evolución temporal
- Bar chart: comercios con mayor riesgo
- Bar chart: tarjetas con mayor riesgo
