# Parte 6 - DAG bajo demanda (Airflow)

DAG creado:
- `fraud_graph_pipeline_on_demand_sbd`

Archivo:
- `airflow/dags/fraud_graph_pipeline_on_demand_sbd.py`
- `SBD/Parte6/trigger_dag_parte6.py`

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
python3 SBD/Parte6/trigger_dag_parte6.py
```

Ejemplo con rango temporal:
```bash
python3 SBD/Parte6/trigger_dag_parte6.py \
  --source-table graph_payments \
  --start-ts 2026-03-14T07:00:00Z \
  --end-ts 2026-03-14T10:00:00Z \
  --graph-name fraud_snapshot_demo
```
