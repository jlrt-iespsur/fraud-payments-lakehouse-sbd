# Parte 7 - Modelo de grafo en Neo4j

Archivos:
- `SBD/Parte7/modelo_grafo_parte7.cypher`
- `SBD/Parte7/consultas_fraude_parte7.cypher`
- `SBD/Parte7/load_graph_parte7.py`
- `SBD/Parte7/run_queries_parte7.py`

Modelo:
- Nodos: `Customer`, `Card`, `Device`, `Merchant`, `Payment`
- Relaciones:
  - `(:Customer)-[:OWNS_CARD]->(:Card)`
  - `(:Card)-[:USED_ON]->(:Device)`
  - `(:Card)-[:AUTHORIZED]->(:Payment)`
  - `(:Payment)-[:AT_MERCHANT]->(:Merchant)`

Detección incluida (consultas Cypher):
- Dispositivos compartidos por múltiples tarjetas
- Tarjetas utilizadas en múltiples países
- Comercios conectados a múltiples entidades sospechosas
- Posibles agrupaciones de comportamiento anómalo

Requisito:
- tener exportados los CSV en `runtime/neo4j/import/<graph_name>` (es decir, previamente hay que ejecutar la Parte 6)

Ejemplo de uso:
```bash
python3 SBD/Parte7/load_graph_parte7.py --graph-name fraud_snapshot_sbd
python3 SBD/Parte7/run_queries_parte7.py --graph-name fraud_snapshot_sbd
```
