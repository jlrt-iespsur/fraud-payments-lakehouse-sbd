# Superset + Trino (Parte 5)

## 1) Levantar servicios

```bash
docker compose up -d trino superset
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
