# Config-Driven PySpark Medallion Pipeline

A locally runnable Bronze → Silver → Gold pipeline with Delta Lake, MinIO, PostgreSQL, and a Streamlit dashboard. Every dataset's behaviour is controlled by a JSON config — zero code changes to onboard new data.

---

## Retail Data Model

```
┌─────────────────────────────┐     ┌──────────────────────────────┐
│         customers           │     │           stores             │
├─────────────────────────────┤     ├──────────────────────────────┤
│ customer_id  STRING  (PK)   │     │ store_id    STRING  (PK)     │
│ first_name   STRING         │     │ store_name  STRING           │
│ last_name    STRING         │     │ city        STRING           │
│ segment      STRING         │     │ state       STRING           │
│ region       STRING         │     │ country     STRING           │
│ city         STRING         │     │ zip_code    LONG             │
│ state        STRING         │     │ store_type  STRING           │
│ country      STRING         │     │   (Online | Offline)         │
│ zip_code     LONG           │     └──────────────┬───────────────┘
│ created_at   DATE           │                    │ 1
└──────────────┬──────────────┘                    │
               │ 1                                 │
               │                                   │
               │ N                                 │ N
┌──────────────▼──────────────────────────────────▼───────────────┐
│                          orders                                  │
├──────────────────────────────────────────────────────────────────┤
│ order_id      STRING  (PK)                                       │
│ customer_id   STRING  (FK → customers)                           │
│ store_id      STRING  (FK → stores)                              │
│ order_date    DATE                                               │
│ status        STRING  (Completed | Returned | Pending …)         │
│ channel       STRING  (Online | In-Store)                        │
│ total_amount  DOUBLE                                             │
└──────────────┬───────────────────────────────────────────────────┘
               │ 1
               │ N  (~3 line items per order on average)
┌──────────────▼───────────────────────────────────────────────────┐
│                      order_line_items                            │
├──────────────────────────────────────────────────────────────────┤
│ line_item_id  STRING  (PK)                                       │
│ order_id      STRING  (FK → orders)                              │
│ product_id    STRING                                             │
│ quantity      LONG                                               │
│ unit_price    DOUBLE                                             │
│ discount      DOUBLE  (0.0–1.0)                                  │
│ line_total    DOUBLE  = quantity × unit_price × (1 − discount)   │
│ created_at    DATE                                               │
└──────────────────────────────────────────────────────────────────┘

Row counts: customers=10K | stores=1K | orders=500K | line_items=1.5M
```

---

## Medallion Architecture

```
 Source Parquet          Bronze (Delta/MinIO)      Silver (Delta/MinIO)     Gold (PostgreSQL)
 ─────────────         ──────────────────────    ───────────────────────   ─────────────────
 customers.parquet  →  bronze/retail_customers/  silver/retail_customers/
 stores.parquet     →  bronze/retail_stores/     silver/retail_stores/
 orders.parquet     →  bronze/retail_orders/     silver/retail_orders_enriched/  → gold_sales_by_store
                                                                                  → gold_sales_by_customer
                                                                                  → gold_daily_trend
 order_line_items   →  bronze/retail_order_      silver/retail_order_items_      → gold_product_performance
   .parquet            line_items/               enriched/

Watermark Delta table: s3a://retail/control/watermark/
```

### Bronze layer rules
- Raw landing zone — no business logic, no type changes
- Adds `ingestion_date = current_date()` as a partition column
- Write mode: dynamic partition overwrite (same-day re-run is safe)

### Silver layer rules
- DQ: null checks, dedup on primary keys, string standardisation, type casting
- `orders_enriched` = orders + customer attrs + store attrs (order-level grain)
- `order_items_enriched` = line items + order header attrs (line-item-level grain)
- Write mode: Delta MERGE on primary key → idempotent upserts

### Gold layer rules
- Always a full recompute from Silver (truncate + insert in PostgreSQL)
- Four aggregated tables consumed by the Streamlit dashboard

### Watermark & idempotency
- Every dataset run writes a row to `s3a://retail/control/watermark/`
- `run_pipeline.py` logs last-run info before each dataset
- `--full-refresh` flag ignores watermark and reprocesses everything
- Bronze re-run on same date → same partition overwritten, no duplicates
- Silver re-run → Delta MERGE, no duplicates
- Gold re-run → truncate + insert, always consistent with current Silver

---

## Project Structure

```
├── configs/
│   ├── bronze/   retail_customers.json  retail_stores.json
│   │             retail_orders.json     retail_order_line_items.json
│   ├── silver/   retail_customers.json  retail_stores.json
│   │             retail_orders_enriched.json  retail_order_items_enriched.json
│   └── gold/     sales_by_store.json    sales_by_customer.json
│                 product_performance.json  daily_trend.json
├── src/
│   ├── readers/        base · csv · parquet · delta · factory
│   ├── transformers/   base · cast_types · drop_duplicates · filter_nulls
│   │                   add_ingestion_date · standardize_strings · factory
│   ├── writers/        base · csv · parquet · delta · postgres · factory
│   ├── core/           config_loader · pipeline · watermark
│   └── di/             container
├── tests/              conftest · test_reader_factory · test_transformers
├── app/
│   └── streamlit_app.py
├── jars/               delta-spark · delta-storage · postgresql JDBC
├── wheels/             PyYAML · pytest · pg8000 (pre-built, no internet needed)
├── run_pipeline.py     Medallion orchestrator
├── main.py             Single-dataset runner (original)
├── Dockerfile
├── Dockerfile.streamlit
├── docker-compose.yml
├── Makefile
└── .env                Credentials (never commit — see .env.example)
```

---

## Quick Start

### 1. Prerequisites
```bash
cp .env.example .env   # already filled with local defaults
make build
```

### 2. Start infrastructure
```bash
make infra
# MinIO console → http://localhost:9001
# PostgreSQL    → localhost:5432 / db=gold
```

### 3. Run the full pipeline
```bash
make run-all
# or individual layers:
make run-bronze
make run-silver
make run-gold

# Force full reprocessing (ignore watermark):
make run-all-fresh
```

### 4. Open the dashboard
```bash
make dashboard
# → http://localhost:8501
```

### 5. Run tests
```bash
make test
```

---

## How to Add a New Dataset (Bronze)

1. Drop the source file into `data/input/`
2. Create `configs/bronze/<table>.json` (copy any existing bronze config as template)
3. Run `make run-bronze` — done. Zero code changes.

## How to Add a New Gold Aggregation

1. Create `configs/gold/<metric>.json` with `aggregation.group_by` and `aggregation.metrics`
2. Run `make run-gold` — the new table appears in PostgreSQL automatically

## How to Add a New Transformer

1. Create `src/transformers/<name>.py` extending `BaseTransformer`
2. Add one line to `src/transformers/factory.py`'s `_REGISTRY` dict
3. Reference it by name in any config file

---

## MinIO Bucket Layout

```
retail/
├── bronze/
│   ├── retail_customers/      _delta_log/ + ingestion_date=YYYY-MM-DD/ partitions
│   ├── retail_stores/
│   ├── retail_orders/
│   └── retail_order_line_items/
├── silver/
│   ├── retail_customers/
│   ├── retail_stores/
│   ├── retail_orders_enriched/
│   └── retail_order_items_enriched/
└── control/
    └── watermark/             Pipeline run tracking (Delta table)
```

---

## Cloud Migration Path (Databricks + ADF)

| Local                        | Cloud equivalent                   |
|------------------------------|------------------------------------|
| MinIO (`s3a://`)             | ADLS Gen2 (`abfss://`)             |
| PostgreSQL (gold)            | Azure SQL / Synapse Analytics      |
| `run_pipeline.py --layer`    | ADF pipeline with parameter        |
| `Container.spark`            | `SparkSession.getActiveSession()`  |
| Docker Compose               | Databricks Job Cluster             |

Config files need only path prefix changes — no application code changes.
