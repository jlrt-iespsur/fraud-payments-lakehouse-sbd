# Parte 5 - Consulta y visualización (Trino + Superset)

Script:
- `SBD/Parte5/check_trino_parte5.py`
- `SBD/Parte5/setup_superset_trino_parte5.py`
- `SBD/Parte5/dashboard_queries_parte5.sql`

Funcionalidad:
1. Verifica acceso a tablas Iceberg desde Trino.
2. Ejecuta consultas de control para métricas clave.
3. Configura conexión Trino en Superset.
4. Deja consultas base para dashboard.

Cómo ejecutar verificación Trino:
```bash
python3 SBD/Parte5/check_trino_parte5.py
```

Cómo configurar Superset con Trino:
```bash
python3 SBD/Parte5/setup_superset_trino_parte5.py \
  --db-name trino_iceberg \
  --trino-uri trino://trino@trino:8080/iceberg/payments
```

Uso en modo dry-run:
```bash
python3 SBD/Parte5/check_trino_parte5.py --dry-run
python3 SBD/Parte5/setup_superset_trino_parte5.py --dry-run
```

Archivos de apoyo:
- `SBD/Parte5/dashboard_queries_parte5.sql`
- `SBD/Parte5/superset_setup_parte5.md`

Nota:
- Superset por defecto en `http://localhost:8088`.
