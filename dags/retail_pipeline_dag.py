"""Retail Medallion Pipeline DAG — 14 tasks across Bronze → Silver → Gold.

Each task runs one dataset through one layer using the spark-pipeline Docker image.
Airflow passes {{ ds }} as --date so each daily run reads the correct facts folder.

DAG dependency graph:

  bronze_customers ──► silver_customers ──┐
  bronze_stores    ──► silver_stores    ──┼──► silver_orders_enriched ──► gold_sales_by_store
  bronze_products  ──► silver_products  ──┤                           ──► gold_sales_by_customer
  bronze_orders    ───────────────────────┘                           ──► gold_daily_trend
       │
       └──► silver_order_items_enriched ──► gold_product_performance
  bronze_order_items ──────────────────────┘   (+ silver_products)
"""
import os

from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PIPELINE_IMAGE = os.getenv("PIPELINE_IMAGE", "config_based_data_processing-spark-pipeline")
NETWORK        = os.getenv("DOCKER_NETWORK",  "config_based_data_processing_default")
PROJECT_DIR    = os.getenv("PROJECT_DIR")
if not PROJECT_DIR:
    raise RuntimeError("PROJECT_DIR env var must be set — absolute path to the cloned repo")

# Environment variables forwarded to every spark-pipeline container
SPARK_ENV = {
    "SPARK_MODE":          "local",
    "MINIO_ENDPOINT":      "http://minio:9000",
    "MINIO_ACCESS_KEY":    "minioadmin",
    "MINIO_SECRET_KEY":    "minioadmin",
    "POSTGRES_HOST":       "postgres",
    "POSTGRES_PORT":       "5432",
    "POSTGRES_DB":         "gold",
    "POSTGRES_USER":       "postgres",
    "POSTGRES_PASSWORD":   "postgres",
    "PYTHONPATH":          "/app:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip",
    "PYSPARK_DRIVER_PYTHON": "python3",
}

# Volumes mounted into every spark-pipeline container
MOUNTS = [
    Mount(target="/data",         source=PROJECT_DIR,                type="bind"),
    Mount(target="/app/configs",  source=f"{PROJECT_DIR}/configs",   type="bind"),
    Mount(target="/app/schemas",  source=f"{PROJECT_DIR}/schemas",   type="bind"),
]

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="retail_medallion_pipeline",
    description="Bronze → Silver → Gold retail pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 3),
    catchup=False,
    default_args=default_args,
    tags=["retail", "medallion"],
) as dag:

    def spark_task(task_id: str, layer: str, dataset: str) -> DockerOperator:
        return DockerOperator(
            task_id=task_id,
            image=PIPELINE_IMAGE,
            command=(
                f"--layer {layer} "
                f"--dataset {dataset} "
                f"--date {{{{ dag_run.conf.get('date', ds) }}}}"
            ),
            network_mode=NETWORK,
            environment=SPARK_ENV,
            mounts=MOUNTS,
            auto_remove=True,
            docker_url="unix://var/run/docker.sock",
        )

    # ── Bronze (5 tasks) ──────────────────────────────────────────────────────
    b_customers   = spark_task("bronze_customers",   "bronze", "retail_customers")
    b_stores      = spark_task("bronze_stores",      "bronze", "retail_stores")
    b_products    = spark_task("bronze_products",    "bronze", "retail_products")
    b_orders      = spark_task("bronze_orders",      "bronze", "retail_orders")
    b_order_items = spark_task("bronze_order_items", "bronze", "retail_order_items")

    # ── Silver (5 tasks) ──────────────────────────────────────────────────────
    s_customers          = spark_task("silver_customers",          "silver", "retail_customers")
    s_stores             = spark_task("silver_stores",             "silver", "retail_stores")
    s_products           = spark_task("silver_products",           "silver", "retail_products")
    s_orders_enriched    = spark_task("silver_orders_enriched",    "silver", "retail_orders")
    s_order_items        = spark_task("silver_order_items_enriched","silver", "retail_order_items")

    # ── Gold (6 tasks) ────────────────────────────────────────────────────────
    g_sales_by_store    = spark_task("gold_sales_by_store",       "gold", "gold_sales_by_store")
    g_sales_by_customer = spark_task("gold_sales_by_customer",    "gold", "gold_sales_by_customer")
    g_daily_trend       = spark_task("gold_daily_trend",          "gold", "gold_daily_trend")
    g_product_perf      = spark_task("gold_product_performance",  "gold", "gold_product_performance")
    g_category_perf     = spark_task("gold_category_performance", "gold", "gold_category_performance")
    g_regional_sales    = spark_task("gold_regional_sales",       "gold", "gold_regional_sales")

    # ── Dependencies ──────────────────────────────────────────────────────────
    # Dims: bronze → silver (independent)
    b_customers   >> s_customers
    b_stores      >> s_stores
    b_products    >> s_products

    # orders_enriched needs bronze_orders + silver dims
    [b_orders, s_customers, s_stores] >> s_orders_enriched

    # order_items_enriched needs bronze_order_items + bronze_orders + silver_products
    [b_order_items, b_orders, s_products] >> s_order_items

    # Gold from orders_enriched
    s_orders_enriched >> [g_sales_by_store, g_sales_by_customer, g_daily_trend, g_regional_sales]

    # Gold from order_items_enriched
    s_order_items >> [g_product_perf, g_category_perf]
