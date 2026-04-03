# Config-Driven PySpark Medallion Pipeline

A locally runnable Bronze → Silver → Gold pipeline with Delta Lake, MinIO, PostgreSQL, Airflow orchestration, and a Streamlit dashboard. Every dataset's behaviour is controlled by a JSON config — zero code changes to onboard new data.

---

## Source Data Model

Five datasets — 3 dimension tables and 2 fact tables.

```
┌──────────────────────────┐   ┌──────────────────────────┐   ┌──────────────────────────┐
│        customers         │   │         stores           │   │        products          │
├──────────────────────────┤   ├──────────────────────────┤   ├──────────────────────────┤
│ customer_id  STRING (PK) │   │ store_id    STRING (PK)  │   │ product_id   STRING (PK) │
│ first_name   STRING      │   │ store_name  STRING       │   │ product_name STRING      │
│ last_name    STRING      │   │ city        STRING       │   │ category     STRING      │
│ segment      STRING      │   │ state       STRING       │   │ unit_price   DOUBLE      │
│ region       STRING      │   │ country     STRING       │   └────────────┬─────────────┘
│ city         STRING      │   │ zip_code    LONG         │                │ 1
│ state        STRING      │   │ store_type  STRING       │                │
│ country      STRING      │   │  (Online|Offline)        │                │ N
│ zip_code     LONG        │   └──────────┬───────────────┘                │
│ created_at   DATE        │              │ 1                               │
└──────────┬───────────────┘              │                                 │
           │ 1                            │ N                               │
           │ N                            │                                 │
           └──────────────────────────────▼─────────────────────────────────┤
                                 ┌────────────────────────────────────────── ┘
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                               orders                                     │
├──────────────────────────────────────────────────────────────────────────┤
│ order_id      STRING  (PK)                                               │
│ customer_id   STRING  (FK → customers)                                   │
│ store_id      STRING  (FK → stores)                                      │
│ order_date    DATE                                                       │
│ status        STRING  (Completed | Returned | Pending | Cancelled)       │
│ channel       STRING  (Online | In-Store)                                │
│ total_amount  DOUBLE                                                     │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │ 1
                               │ N  (~3 line items per order on average)
┌──────────────────────────────▼───────────────────────────────────────────┐
│                          order_line_items                                │
├──────────────────────────────────────────────────────────────────────────┤
│ line_item_id  STRING  (PK)                                               │
│ order_id      STRING  (FK → orders)                                      │
│ product_id    STRING  (FK → products)                                    │
│ quantity      LONG                                                       │
│ unit_price    DOUBLE                                                     │
│ discount      DOUBLE  (0.0–1.0)                                          │
│ line_total    DOUBLE  = quantity × unit_price × (1 − discount)           │
│ created_at    DATE                                                       │
└──────────────────────────────────────────────────────────────────────────┘

Row counts: customers=10K | stores=1K | products=5K | orders=500K | line_items~1.5M
```

---

## Medallion Architecture

```
Source (Parquet)                Bronze (Delta/MinIO)              Silver (Delta/MinIO)              Gold (PostgreSQL · gold_schema)
────────────────                ────────────────────              ────────────────────              ───────────────────────────────
dims/customers.parquet      →   bronze/retail_customers/      →   silver/retail_customers/
dims/stores.parquet         →   bronze/retail_stores/         →   silver/retail_stores/
dims/products.parquet       →   bronze/retail_products/       →   silver/retail_products/
facts/{y}/{m}/{d}/              bronze/retail_orders/         →   silver/retail_orders_enriched/   →  gold_sales_by_store
  orders.parquet          →                                                                        →  gold_sales_by_customer
                                                                                                   →  gold_daily_trend
                                                                                                   →  gold_regional_sales
facts/{y}/{m}/{d}/              bronze/retail_order_          →   silver/retail_order_items_       →  gold_product_performance
  order_line_items.parquet →      line_items/                       enriched/                     →  gold_category_performance

Watermark Delta table: s3a://retail/control/watermark/
```

### Bronze
- Raw landing zone — no business logic, no type changes
- Adds `ingestion_date = current_date()` as a partition column
- Facts are date-partitioned by source folder (`facts/YYYY/MM/DD/`)
- Write mode: dynamic partition overwrite — same-day rerun is safe

### Silver
- DQ: null checks on primary keys, dedup, string standardisation, type casting
- Schema enforced from `schemas/silver/<dataset>.json` — fail or warn on violations
- `orders_enriched` = orders + customer (segment, region) + store (name, type)
- `order_items_enriched` = line items + order header + product (name, category)
- Write mode: Delta MERGE on primary key → idempotent upserts

### Gold
- Full recompute from Silver every run (truncate + insert in PostgreSQL `gold_schema`)
- Pure aggregations — no joins, no DQ, just `GROUP BY + metrics`
- Six tables consumed by the Streamlit dashboard

### Watermark & idempotency
- Every dataset writes a row to `s3a://retail/control/watermark/` after each run
- Same-day rerun → skipped automatically (watermark check)
- `--full-refresh` flag ignores watermark and reprocesses everything
- Bronze rerun → same partition overwritten, no duplicates
- Silver rerun → Delta MERGE, no duplicates
- Gold rerun → truncate + insert, always consistent with Silver

---

## Project Structure

```
├── configs/
│   ├── jobs/       job_01_retail_customers_config.json   (bronze + silver in one file)
│   │               job_02_retail_stores_config.json
│   │               job_03_retail_products_config.json
│   │               job_04_retail_orders_config.json
│   │               job_05_retail_order_items_config.json
│   └── gold/       sales_by_store.json   sales_by_customer.json
│                   product_performance.json   daily_trend.json
│                   category_performance.json  regional_sales.json
├── schemas/
│   └── silver/     retail_customers.json   retail_stores.json
│                   retail_products.json    retail_orders_enriched.json
│                   retail_order_items_enriched.json
├── src/
│   ├── readers/        base · csv · parquet · delta · factory
│   ├── transformers/   base · cast_types · drop_duplicates · filter_nulls
│   │                   add_ingestion_date · standardize_strings · factory
│   ├── writers/        base · delta · postgres · factory
│   ├── core/           config_loader · pipeline · watermark · schema_enforcer
│   └── di/             container
├── dags/
│   └── retail_pipeline_dag.py   (16-task Airflow DAG)
├── tests/              conftest · test_reader_factory · test_transformers
├── app/
│   └── streamlit_app.py         (6-page dashboard)
├── jars/               delta-spark · delta-storage · postgresql JDBC
├── wheels/             pre-built Python wheels (no internet needed in Docker)
├── generate_data.py    synthetic data generator
├── run_pipeline.py     medallion orchestrator
├── Dockerfile          spark-pipeline image
├── Dockerfile.streamlit
├── Dockerfile.airflow
├── docker-compose.yml
├── Makefile
├── .env.example
└── .gitignore
```

---

## Quick Start

### 1. Clone and configure
```bash
git clone <repo-url>
cd config_based_data_processing
cp .env.example .env
# Edit .env — set PROJECT_DIR to the absolute path of this repo
```

### 2. Generate source data
```bash
pip install pandas pyarrow faker
python generate_data.py           # generates retail/dims/ and retail/facts/today/
```

### 3. Build images
```bash
make build
```

### 4. Start infrastructure
```bash
make infra
# MinIO console → http://localhost:9001  (minioadmin / minioadmin)
# PostgreSQL    → localhost:5432 / db=gold
```

### 5. Run the full pipeline
```bash
make run-all

# Individual layers:
make run-bronze
make run-silver
make run-gold

# Force full reprocessing (ignore watermark):
make run-all-fresh
```

### 6. Open the dashboard
```bash
make dashboard
# → http://localhost:8501
```

### 7. Run tests
```bash
make test
```

### 8. Start Airflow (optional)
```bash
make airflow
# → http://localhost:8081  (admin / admin)
# Trigger DAG: retail_medallion_pipeline
```

---

## Job Config Format

One file per dataset covering both bronze and silver:

```json
{
  "dataset": "retail_orders",
  "bronze": {
    "source_format": "parquet",
    "source_path": "/data/retail/facts/{year}/{month}/{day}/orders.parquet",
    "target_path": "s3a://retail/bronze/retail_orders/"
  },
  "silver": {
    "source_path": "s3a://retail/bronze/retail_orders/",
    "target_path": "s3a://retail/silver/retail_orders_enriched/",
    "schema_path": "/app/schemas/silver/retail_orders_enriched.json",
    "primary_key": "order_id",
    "not_null_columns": ["order_id", "customer_id"],
    "dedup_key": ["order_id"],
    "on_null_violation": "warn",
    "joins": [...]
  }
}
```

Gold configs are separate (aggregation-only):

```json
{
  "dataset": "gold_daily_trend",
  "source": { "format": "delta", "path": "s3a://retail/silver/retail_orders_enriched/" },
  "aggregation": {
    "group_by": ["order_date"],
    "metrics": [
      { "name": "total_revenue", "expr": "sum(total_amount)" },
      { "name": "order_count",   "expr": "count(order_id)" }
    ]
  },
  "sink": { "format": "postgres", "schema": "gold_schema", "table": "gold_daily_trend", "mode": "overwrite" }
}
```

---

## Schema File Format

One JSON file per silver dataset in `schemas/silver/`:

```json
[
  { "column_name": "order_id",     "datatype": "string", "nullable": false },
  { "column_name": "total_amount", "datatype": "double", "nullable": true  }
]
```

Enforced at runtime: columns cast to declared types, non-nullable columns checked, dataframe pruned to schema-defined columns only.

---

## How to Add a New Dataset

1. Create `configs/jobs/job_NN_<name>_config.json` (copy any existing job config)
2. Create `schemas/silver/<name>.json` with column definitions
3. Run `make run-all` — zero code changes

## How to Add a New Gold Aggregation

1. Create `configs/gold/<metric>.json` with `aggregation.group_by` and `aggregation.metrics`
2. Run `make run-gold` — new table appears in `gold_schema` automatically

## How to Add a New Transformer

1. Create `src/transformers/<name>.py` extending `BaseTransformer`
2. Add one line to `src/transformers/factory.py`'s `_REGISTRY`
3. Reference by name in any job config

---

## MinIO Bucket Layout

```
retail/
├── bronze/
│   ├── retail_customers/        _delta_log/ + ingestion_date=YYYY-MM-DD/ partitions
│   ├── retail_stores/
│   ├── retail_products/
│   ├── retail_orders/
│   └── retail_order_line_items/
├── silver/
│   ├── retail_customers/
│   ├── retail_stores/
│   ├── retail_products/
│   ├── retail_orders_enriched/
│   └── retail_order_items_enriched/
└── control/
    └── watermark/               Pipeline run tracking (Delta table)
```

## PostgreSQL Schema Layout

```
gold (database)
├── gold_schema
│   ├── gold_daily_trend
│   ├── gold_sales_by_store
│   ├── gold_sales_by_customer
│   ├── gold_product_performance
│   ├── gold_category_performance
│   └── gold_regional_sales
└── silver_schema               (reserved for future silver summary tables)
```

---

## Airflow DAG

16 tasks with explicit data dependencies:

```
bronze_customers  → silver_customers  ─┐
bronze_stores     → silver_stores     ─┼→ silver_orders_enriched → gold_sales_by_store
bronze_products   → silver_products   ─┘                         → gold_sales_by_customer
bronze_orders     ──────────────────────┘                         → gold_daily_trend
                                                                  → gold_regional_sales
bronze_order_items ──────────────────────→ silver_order_items    → gold_product_performance
 (+ bronze_orders, silver_products)          _enriched           → gold_category_performance
```

Each task calls `run_pipeline.py --layer <layer> --dataset <dataset> --date {{ ds }}`.
Watermark ensures idempotency — tasks already run today are skipped automatically.

---

## Cloud Migration Path

| Local | Cloud equivalent |
|-------|-----------------|
| MinIO (`s3a://`) | ADLS Gen2 (`abfss://`) or S3 |
| PostgreSQL `gold_schema` | Azure SQL / Synapse / Redshift |
| Airflow on Docker | MWAA / Cloud Composer / Databricks Workflows |
| `run_pipeline.py --layer` | ADF pipeline with layer parameter |
| Docker Compose | Databricks Job Cluster |

Only path prefixes in `.env` need changing — no application code changes.
