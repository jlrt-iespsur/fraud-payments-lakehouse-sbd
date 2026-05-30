SHELL := /bin/bash

.PHONY: help setup-env start restart stop clean bronze generator silver gold parte10-run parte11-run parte12-run trino

help:
	@echo "Targets disponibles:"
	@echo "  setup-env  - crea .env a partir de .env.example si no existe"
	@echo "  start      - arranca toda la plataforma"
	@echo "  restart    - reinicia la plataforma"
	@echo "  stop       - detiene los contenedores"
	@echo "  clean      - elimina datos locales de runtime"
	@echo "  bronze     - arranca el job Bronze"
	@echo "  generator  - lanza el generador de eventos"
	@echo "  silver     - ejecuta la transformacion Silver"
	@echo "  gold       - ejecuta la transformacion Gold"
	@echo "  parte10-run- ejecuta la Parte10 (S3 + RDS)"
	@echo "  parte11-run- ejecuta la Parte11 (analisis Athena-like)"
	@echo "  parte12-run- ejecuta la Parte12 (fine-tuning HF)"
	@echo "  trino      - abre la CLI de Trino"

setup-env:
	@if [ ! -f .env ]; then cp .env.example .env; echo ".env creado desde .env.example"; else echo ".env ya existe"; fi

start:
	./scripts/start_all.sh

restart:
	./scripts/restart_all.sh

stop:
	docker compose down

clean:
	./scripts/clean_all.sh

bronze:
	./scripts/run_bronze.sh

generator:
	./scripts/run_generator.sh --events 5000 --sleep-ms 150

silver:
	./scripts/run_silver.sh

gold:
	./scripts/run_gold.sh

parte10-run:
	./SBD/Parte10/run_parte10.sh $(ARGS)

parte11-run:
	./SBD/Parte11/run_parte11.sh $(ARGS)

parte12-run:
	./SBD/Parte12/run_parte12.sh $(ARGS)

trino:
	docker compose exec trino trino
