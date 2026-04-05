# Parte 6 - DAG bajo demanda (Airflow)

DAG creado:
- `fraud_graph_pipeline_on_demand_sbd`

Archivo:
- `airflow/dags/fraud_graph_pipeline_on_demand_sbd.py`

Flujo:
1. compacta tabla Iceberg origen
2. prepara dataset tabular para grafo (con filtro temporal)
3. exporta CSV para Neo4j
4. carga CSV en Neo4j

Parámetros del DAG:
- `source_table`
- `start_ts`
- `end_ts`
- `graph_name`

Trigger rápido:
```bash
./SBD/Parte6/trigger_dag_parte6.sh
```

Ejemplo con rango temporal:
```bash
SOURCE_TABLE=graph_payments \
START_TS=2026-03-14T07:00:00Z \
END_TS=2026-03-14T10:00:00Z \
GRAPH_NAME=fraud_snapshot_demo \
./SBD/Parte6/trigger_dag_parte6.sh
```
