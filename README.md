# Fraud Payments Lakehouse

Plataforma de detección de fraude en pagos con arquitectura Lakehouse, procesamiento casi en tiempo real y vista relacional y de grafo sobre el mismo dominio.

## Resumen

El proyecto implementa un flujo completo para:

- generar eventos sintéticos de pago en Kafka
- persistir una capa Bronze en Iceberg sobre MinIO con Spark Streaming
- enriquecer el dato en Silver con variables temporales y de comportamiento
- calcular alertas interpretables en Gold con `risk_score` y motivos
- consultar el Lakehouse mediante Trino
- orquestar exportaciones con Airflow
- investigar relaciones sospechosas con Neo4j

## Arquitectura

```text
Generator -> Kafka -> Spark Bronze -> Iceberg/MinIO
                                   -> Spark Silver -> Iceberg/MinIO
                                   -> Spark Gold   -> Iceberg/MinIO

Trino ------> consultas SQL sobre Iceberg
Superset ---> analítica y dashboards
Airflow ----> compactación/exportación de dataset de grafo
Neo4j ------> investigación relacional y de patrones sospechosos
```

## Stack tecnológico

- Apache Kafka
- Apache Spark 3.5
- Apache Iceberg
- MinIO
- Trino
- Apache Airflow
- Apache Superset
- Neo4j
- PostgreSQL
- Docker Compose

## Estructura del repositorio

```text
.
├── airflow/                 # DAGs de orquestación
├── apps/
│   ├── generator/           # Generador sintético de eventos
│   └── spark/               # Jobs Bronze, Silver y Gold
├── config/                  # Configuración de Trino y otros servicios
├── docker/                  # Dockerfiles personalizados
├── docs/                    # Manuales, memoria, presentación y anexos
├── neo4j/cypher/            # Carga y consultas Cypher
├── orchestration/           # Tareas reutilizables de Airflow
├── scripts/                 # Scripts de operación
├── sql/                     # Consultas de validación
└── superset/                # Consultas auxiliares de dashboard
```

## Requisitos

- Docker Desktop
- Docker Compose
- macOS, Linux o Windows con virtualización activa
- Recomendado: al menos 8 GB de RAM asignables a Docker

Dependencias Python globales del proyecto:

```bash
pip install -r requirements.txt
```

Se han observado problemas de rendimiento en entornos con 8 GB de RAM,
es recomendable operar por fases y parar servicios no necesarios durante la ejecución.

## Puesta en marcha

1. Prepara variables de entorno:

```bash
cp .env.example .env
```

2. Arranca toda la plataforma:

```bash
./scripts/start_all.sh
```

3. Ejecuta el flujo principal:

```bash
./scripts/run_bronze.sh
./scripts/run_generator.sh --events 5000 --sleep-ms 150
./scripts/run_silver.sh
./scripts/run_gold.sh
```

Alternativamente:

```bash
make setup-env
make start
make bronze
make generator
make silver
make gold
```

## Scripts de operación

Estos son los scripts principales del proyecto y el momento en que conviene usarlos:

### `./scripts/start_all.sh`

Levanta toda la infraestructura base del proyecto.
- crea directorios de `runtime` si no existen
- construye las imágenes locales necesarias
- arranca los servicios principales
- ejecuta los contenedores de inicialización
- deja listas las interfaces web y la base para el pipeline

Cuándo usarlo:

- al empezar desde cero
- después de clonar el repositorio
- cuando quieres volver a dejar toda la plataforma lista para trabajar

### `./scripts/restart_all.sh`

Reinicia la plataforma completa sin borrar datos persistidos.
- ejecuta `docker compose down`
- vuelve a llamar a `./scripts/start_all.sh`

Cuándo usarlo:

- si varios servicios han quedado inestables
- si quieres reiniciar el stack entero sin hacer una limpieza completa

### `./scripts/clean_all.sh`

Borra el entorno local y deja el proyecto preparado para un arranque limpio.
- baja contenedores y elimina volúmenes del proyecto
- borra datos de `runtime`
- recrea la estructura mínima de carpetas

Cuándo usarlo:

- si quieres empezar completamente desde cero
- si el estado local ha quedado corrupto o inconsistente
- antes de una prueba limpia de entrega o validación final

### `./scripts/run_bronze.sh`

Lanza el job de Spark Streaming que consume Kafka y escribe Bronze en Iceberg.
- ejecuta `bronze_to_iceberg.py` dentro del contenedor `spark`
- añade los paquetes Spark, Iceberg y PostgreSQL necesarios
- fija la región S3 compatible para MinIO

Cuándo usarlo:

- después de `start_all.sh`
- antes de generar eventos
- mientras quieras que Bronze siga consumiendo Kafka

Importante:

- se queda en ejecución
- debe mantenerse abierto en su propia terminal durante la ingesta

### `./scripts/run_generator.sh`

Genera eventos sintéticos de pago y los publica en Kafka.
- ejecuta `payment_event_generator.py` dentro del contenedor `generator`
- admite parámetros como `--events` y `--sleep-ms`

Cuándo usarlo:

- cuando Bronze ya está escuchando
- para poblar el pipeline con datos de demo
- para repetir pruebas con distintos volúmenes o ritmos

### `./scripts/run_silver.sh`

Construye la capa Silver a partir de Bronze.
- ejecuta `silver_enrichment.py` en Spark
- tipa campos
- elimina duplicados
- calcula variables de comportamiento y ventana temporal

Cuándo usarlo:

- después de haber generado suficientes eventos
- cuando Bronze ya ha escrito datos en Iceberg
- cada vez que quieras recalcular Silver con el estado actual de Bronze

Importante:

- es un job batch
- termina y devuelve el prompt

### `./scripts/run_gold.sh`

Construye la capa Gold a partir de Silver.
- ejecuta `gold_fraud_detection.py` en Spark
- aplica reglas de fraude
- genera `fraud_alerts`
- genera `graph_payments`

Cuándo usarlo:

- después de ejecutar Silver
- cuando quieras recalcular alertas y preparar el dataset de grafo

Importante:

- también es un job batch
- termina y devuelve el prompt

## Orden recomendado de uso

Para una ejecución normal o una demo, el orden correcto es este:

1. `cp .env.example .env`
2. `./scripts/start_all.sh`
3. `./scripts/run_bronze.sh`
4. `./scripts/run_generator.sh --events 5000 --sleep-ms 150`
5. `./scripts/run_silver.sh`
6. `./scripts/run_gold.sh`

Para reiniciar sin borrar datos:

1. `./scripts/restart_all.sh`
2. volver a lanzar los jobs que necesites

Para empezar desde cero:

1. `./scripts/clean_all.sh`
2. `./scripts/start_all.sh`
3. repetir el flujo normal

## Interfaces disponibles

- MinIO: `http://localhost:9001`
- Trino: `http://localhost:8080`
- Kafka UI: `http://localhost:8082`
- Spark UI: `http://localhost:4040-4050`
- Superset: `http://localhost:8088`
- Airflow: `http://localhost:8081`
- Neo4j Browser: `http://localhost:7474`

Credenciales por defecto para demo local:

- Airflow: `admin / admin`
- Superset: `admin / admin`
- Neo4j: `neo4j / neo4j_password`

## Flujo operativo recomendado

### 1. Capa Bronze

- Mantener `./scripts/run_bronze.sh` activo
- Ver mensajes en Kafka UI:
  - `local -> Topics -> payments -> Messages`
- Ver micro-batches en Spark UI:
  - `Structured Streaming`

### 2. Capa Silver

- Ejecutar `./scripts/run_silver.sh`
- Consultar con Trino:

```sql
SELECT count(*) FROM iceberg.payments.silver_payments;
```

### 3. Capa Gold

- Ejecutar `./scripts/run_gold.sh`
- Validar alertas:

```sql
SELECT count(*) FROM iceberg.payments.fraud_alerts;
SELECT payment_id, card_id, risk_score, reasons_text
FROM iceberg.payments.fraud_alerts
ORDER BY risk_score DESC
LIMIT 20;
```

### 4. Grafo de investigación

- Lanzar el DAG `fraud_graph_pipeline` en Airflow
- Validar nodos y relaciones en Neo4j Browser

## Resultados validados en la demo de referencia

- Bronze: `7443` registros
- Silver: `7299` registros
- Gold (`fraud_alerts`): `7049` alertas

## Operación y mantenimiento

Parar servicios pesados cuando no se necesiten:

```bash
docker compose stop spark generator kafka-ui superset
```

Detener toda la plataforma:

```bash
docker compose down
```

Limpiar runtime local:

```bash
./scripts/clean_all.sh
```

## Documentación

- [Manual de uso](docs/MANUAL_DE_USO.md)
- [Documentación técnica completa](docs/DOCUMENTACION_FINAL.md)
- [Informe de resultados y conclusiones](docs/INFORME_RESULTADOS_Y_CONCLUSIONES.md)
- [Anexo de soluciones a errores](docs/ANEXO_SOLUCIONES_A_POSIBLES_ERRORES.md)
- [Presentación del proyecto](docs/PRESENTACION_PROYECTO.html)
- [Referencias](docs/REFERENCIAS.md)

## Seguridad

Este repositorio está orientado a demostración y entorno local. No uses las credenciales de ejemplo en entornos reales.

## Licencia

Este proyecto se distribuye bajo licencia [MIT](LICENSE).
