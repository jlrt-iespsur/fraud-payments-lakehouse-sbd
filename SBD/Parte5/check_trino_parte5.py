#!/usr/bin/env python3
"""
Parte 5 - Verificacion Trino + Iceberg para consultas de dashboard.
"""

import argparse
import os
import subprocess
from pathlib import Path


root_dir = Path(__file__).resolve().parents[2]
os.chdir(root_dir)

parser = argparse.ArgumentParser(description="Comprueba consultas clave en Trino para la Parte 5")
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Muestra comandos sin ejecutarlos",
)
args = parser.parse_args()

commands = [
    ("==> Verificando Trino + Iceberg", ["docker", "compose", "up", "-d", "trino"]),
    ("==> Esquemas del catalogo iceberg", ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", "SHOW SCHEMAS FROM iceberg"]),
    ("==> Tablas en iceberg.payments", ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", "SHOW TABLES FROM iceberg.payments"]),
    ("==> Conteo total de transacciones (silver)", ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", "SELECT count(*) AS total_transactions FROM iceberg.payments.silver_payments"]),
    ("==> Conteo total de alertas (gold)", ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", "SELECT count(*) AS total_fraud_alerts FROM iceberg.payments.fraud_alerts"]),
    (
        "==> Evolucion temporal de alertas",
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "trino",
            "trino",
            "--execute",
            "SELECT date_trunc('hour', event_time) AS hour_bucket, count(*) AS total_alerts "
            "FROM iceberg.payments.fraud_alerts GROUP BY 1 ORDER BY 1 LIMIT 24",
        ],
    ),
    (
        "==> Comercios con mayor riesgo",
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "trino",
            "trino",
            "--execute",
            "SELECT merchant_id, avg(risk_score) AS avg_risk_score, count(*) AS alert_count "
            "FROM iceberg.payments.fraud_alerts GROUP BY 1 ORDER BY avg_risk_score DESC, alert_count DESC LIMIT 10",
        ],
    ),
    (
        "==> Tarjetas con mayor riesgo",
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "trino",
            "trino",
            "--execute",
            "SELECT card_id, avg(risk_score) AS avg_risk_score, count(*) AS alert_count "
            "FROM iceberg.payments.fraud_alerts GROUP BY 1 ORDER BY avg_risk_score DESC, alert_count DESC LIMIT 10",
        ],
    ),
]

for title, command in commands:
    print()
    print(title)
    print(" ".join(command))
    if not args.dry_run:
        subprocess.run(command, check=True)
