from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from orchestration.lakehouse_tasks import (
    build_graph_dataset,
    compact_table,
    export_graph_dataset,
    load_graph_into_neo4j,
)


def compact_table_task(**context) -> None:
    compact_table(context["params"]["source_table"])


def build_graph_dataset_task(**context) -> dict[str, str]:
    params = context["params"]
    staging_table = build_graph_dataset(
        source_table=params["source_table"],
        graph_name=params["graph_name"],
        start_ts=params.get("start_ts"),
        end_ts=params.get("end_ts"),
    )
    return {"staging_table": staging_table}


def export_graph_task(**context) -> dict[str, int]:
    params = context["params"]
    xcom_payload = context["ti"].xcom_pull(task_ids="prepare_graph_dataset")
    staging_table = xcom_payload.get("staging_table", "graph_payments") if xcom_payload else "graph_payments"
    return export_graph_dataset(
        graph_name=params["graph_name"],
        source_table=staging_table,
    )


def load_graph_task(**context) -> None:
    load_graph_into_neo4j(context["params"]["graph_name"])


with DAG(
    dag_id="fraud_graph_pipeline_on_demand_sbd",
    description="Orquestacion manual SBD: compactar, preparar dataset de grafo, exportar y cargar Neo4j",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)},
    params={
        "source_table": Param("graph_payments", type="string"),
        "start_ts": Param("", type=["null", "string"]),
        "end_ts": Param("", type=["null", "string"]),
        "graph_name": Param("fraud_snapshot_sbd", type="string"),
    },
    tags=["fraud", "lakehouse", "neo4j", "sbd"],
) as dag:
    compact = PythonOperator(
        task_id="compact_iceberg_table",
        python_callable=compact_table_task,
    )

    prepare = PythonOperator(
        task_id="prepare_graph_dataset",
        python_callable=build_graph_dataset_task,
    )

    export = PythonOperator(
        task_id="export_graph_dataset",
        python_callable=export_graph_task,
    )

    load = PythonOperator(
        task_id="load_graph_to_neo4j",
        python_callable=load_graph_task,
    )

    compact >> prepare >> export >> load
