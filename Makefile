.PHONY: build infra run-bronze run-silver run-gold run-all run-all-fresh test dashboard airflow airflow-stop clean

# ── Infrastructure ──────────────────────────────────────────────────────────

build:
	docker compose build

# Start MinIO + PostgreSQL + create schemas (idempotent)
infra:
	docker compose up -d minio postgres minio-init postgres-init
	@echo "MinIO console → http://localhost:9001  (minioadmin / minioadmin)"
	@echo "PostgreSQL    → localhost:5432"

# ── Medallion pipeline ───────────────────────────────────────────────────────

run-bronze: infra
	docker compose run --rm spark-pipeline python run_pipeline.py --layer bronze

run-silver: infra
	docker compose run --rm spark-pipeline python run_pipeline.py --layer silver

run-gold: infra
	docker compose run --rm spark-pipeline python run_pipeline.py --layer gold

run-all: infra
	docker compose run --rm spark-pipeline python run_pipeline.py --layer all

run-all-fresh: infra
	docker compose run --rm spark-pipeline python run_pipeline.py --layer all --full-refresh

# ── Dashboard ────────────────────────────────────────────────────────────────

dashboard: infra
	docker compose up streamlit
	@echo "Dashboard → http://localhost:8501"

# ── Tests ────────────────────────────────────────────────────────────────────

test: build
	docker compose run --rm spark-test

# ── Airflow ──────────────────────────────────────────────────────────────────

airflow: infra build
	docker compose up -d airflow-db airflow-init
	docker compose up -d airflow-webserver airflow-scheduler
	@echo "Airflow UI → http://localhost:8081  (admin / admin)"

airflow-stop:
	docker compose stop airflow-webserver airflow-scheduler airflow-db

# ── Cleanup ──────────────────────────────────────────────────────────────────

clean:
	docker compose down --rmi local -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
