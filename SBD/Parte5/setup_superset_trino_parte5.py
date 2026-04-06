#!/usr/bin/env python3
"""
Parte 5 - Configuracion de Superset con Trino.
"""

import argparse
import os
import subprocess
from pathlib import Path


root_dir = Path(__file__).resolve().parents[2]
os.chdir(root_dir)

parser = argparse.ArgumentParser(description="Configura conexion Trino en Superset")
parser.add_argument(
    "--db-name",
    default=os.getenv("DB_NAME", "trino_iceberg"),
    help="Nombre de la conexion en Superset",
)
parser.add_argument(
    "--trino-uri",
    default=os.getenv("TRINO_URI", "trino://trino@trino:8080/iceberg/payments"),
    help="URI SQLAlchemy para Trino",
)
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Muestra comandos sin ejecutarlos",
)
args = parser.parse_args()

print("==> Levantando Trino y Superset")
command_up = ["docker", "compose", "up", "-d", "trino", "superset"]
print(" ".join(command_up))
if not args.dry_run:
    subprocess.run(command_up, check=True)

print()
print("==> Configurando conexion en Superset")
command_set = [
    "docker",
    "compose",
    "exec",
    "-T",
    "superset",
    "superset",
    "set-database-uri",
    "-d",
    args.db_name,
    "-u",
    args.trino_uri,
]
print(" ".join(command_set))
if not args.dry_run:
    subprocess.run(command_set, check=True)

print()
print("==> Probando conexion (driver + dialecto + conectividad)")
command_test = [
    "docker",
    "compose",
    "exec",
    "-T",
    "superset",
    "superset",
    "test-db",
    args.trino_uri,
]
print("printf 'n\\n' | " + " ".join(command_test))
if not args.dry_run:
    subprocess.run(command_test, input="n\n", text=True, check=True)

print()
print("Conexion lista en Superset:")
print(f"- Nombre DB: {args.db_name}")
print(f"- URI: {args.trino_uri}")
