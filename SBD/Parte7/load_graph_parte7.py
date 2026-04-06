#!/usr/bin/env python3
import argparse
import os
import subprocess
import tempfile
import time
from pathlib import Path


root_dir = Path(__file__).resolve().parents[2]
os.chdir(root_dir)

parser = argparse.ArgumentParser(description="Carga el modelo de la Parte 7 en Neo4j")
parser.add_argument(
    "--graph-name",
    default=os.getenv("GRAPH_NAME", "fraud_snapshot_sbd"),
    help="Nombre del snapshot en runtime/neo4j/import/<graph_name>",
)
parser.add_argument(
    "--neo4j-password",
    default=os.getenv("NEO4J_PASSWORD", "neo4j_password"),
    help="Password de Neo4j",
)
args = parser.parse_args()

graph_name = args.graph_name
neo4j_password = args.neo4j_password
graph_dir = root_dir / "runtime" / "neo4j" / "import" / graph_name

if not graph_dir.is_dir():
    raise SystemExit(f"No existe {graph_dir}\nUsa --graph-name con un snapshot valido.")

template_path = root_dir / "SBD" / "Parte7" / "modelo_grafo_parte7.cypher"
template_text = template_path.read_text(encoding="utf-8")
cypher_text = template_text.replace("<graph_name>", graph_name)

with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as temp_file:
    temp_file.write(cypher_text)
    temp_path = temp_file.name

try:
    subprocess.run(["docker", "compose", "up", "-d", "neo4j"], check=True)
    ready = False
    for _ in range(30):
        check = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "neo4j",
                "cypher-shell",
                "-u",
                "neo4j",
                "-p",
                neo4j_password,
                "RETURN 1;",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if check.returncode == 0:
            ready = True
            break
        time.sleep(2)
    if not ready:
        raise SystemExit("Neo4j no arranco a tiempo.")

    with open(temp_path, "rb") as handle:
        subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "neo4j",
                "cypher-shell",
                "-u",
                "neo4j",
                "-p",
                neo4j_password,
            ],
            stdin=handle,
            check=True,
        )
    print(f"Modelo cargado en Neo4j para graph_name={graph_name}")
finally:
    Path(temp_path).unlink(missing_ok=True)
