#!/usr/bin/env python3

# Trigger manual del DAG en Airflow

"""
Uso rapido:
  python3 SBD/Parte6/trigger_dag_parte6.py

Tambien se pueden pasar parametros por flags o por variables de entorno:
  SOURCE_TABLE=graph_payments \
  START_TS=2026-03-14T07:00:00Z \
  END_TS=2026-03-14T10:00:00Z \
  GRAPH_NAME=fraud_snapshot_demo \
  python3 SBD/Parte6/trigger_dag_parte6.py
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


root_dir = Path(__file__).resolve().parents[2]
os.chdir(root_dir)

parser = argparse.ArgumentParser(
    description="Lanza el DAG fraud_graph_pipeline_on_demand_sbd en Airflow",
)
parser.add_argument(
    "--source-table",
    default=os.getenv("SOURCE_TABLE", "graph_payments"),
    help="Tabla origen en Trino/Iceberg (default: graph_payments)",
)
parser.add_argument(
    "--start-ts",
    default=os.getenv("START_TS", ""),
    help="Inicio ISO8601 opcional, por ejemplo 2026-03-14T07:00:00Z",
)
parser.add_argument(
    "--end-ts",
    default=os.getenv("END_TS", ""),
    help="Fin ISO8601 opcional, por ejemplo 2026-03-14T10:00:00Z",
)
parser.add_argument(
    "--graph-name",
    default=os.getenv("GRAPH_NAME", "fraud_snapshot_sbd"),
    help="Nombre del snapshot/grafo (default: fraud_snapshot_sbd)",
)
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Sólo muestra el comando y el conf, sin ejecutar nada",
)
args = parser.parse_args()

conf = {
    "source_table": args.source_table,
    "start_ts": args.start_ts,
    "end_ts": args.end_ts,
    "graph_name": args.graph_name,
}
conf_json = json.dumps(conf, ensure_ascii=True)

command = [
    "docker",
    "compose",
    "exec",
    "-T",
    "airflow-webserver",
    "airflow",
    "dags",
    "trigger",
    "fraud_graph_pipeline_on_demand_sbd",
    "--conf",
    conf_json,
]

print("DAG: fraud_graph_pipeline_on_demand_sbd")
print("Conf:", conf_json)
print("Comando:", " ".join(command))

if args.dry_run:
    print("Dry run: no se ejecutó el trigger.")
    sys.exit(0)

subprocess.run(command, check=True)
print("Trigger lanzado correctamente.")
