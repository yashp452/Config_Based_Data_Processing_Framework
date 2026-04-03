"""Generate synthetic retail parquet files for local development.

Usage:
    pip install pandas pyarrow faker
    python generate_data.py

Output:
    retail/dims/customers.parquet   (10,000 rows)
    retail/dims/stores.parquet      (1,000 rows)
    retail/dims/products.parquet    (5,000 rows)
    retail/facts/<year>/<month>/<day>/orders.parquet          (500,000 rows)
    retail/facts/<year>/<month>/<day>/order_line_items.parquet (~1.5M rows)

Run date defaults to today. Pass --date YYYY-MM-DD to generate for a specific day.
"""
import argparse
import os
import random
import uuid
from datetime import date, timedelta

import pandas as pd

random.seed(42)

SEGMENTS  = ["Consumer", "Corporate", "Home Office"]
REGIONS   = ["East", "West", "Central", "South"]
COUNTRIES = ["US"]
STATES    = ["CA", "TX", "NY", "FL", "IL", "WA", "GA", "OH", "PA", "NC"]
CITIES    = ["Los Angeles", "Houston", "New York", "Miami", "Chicago",
             "Seattle", "Atlanta", "Columbus", "Philadelphia", "Charlotte"]
STORE_TYPES = ["Online", "Offline"]
STATUSES    = ["Completed", "Returned", "Pending", "Cancelled"]
CHANNELS    = ["Online", "In-Store"]
CATEGORIES  = ["Electronics", "Apparel", "Grocery", "Sports", "Home", "Beauty", "Toys"]


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8].upper()}"


def generate_customers(n: int = 10_000) -> pd.DataFrame:
    start = date(2019, 1, 1)
    rows = []
    for _ in range(n):
        i = random.randint(0, len(CITIES) - 1)
        rows.append({
            "customer_id": make_id("CUST"),
            "first_name":  f"First{random.randint(1, 9999)}",
            "last_name":   f"Last{random.randint(1, 9999)}",
            "segment":     random.choice(SEGMENTS),
            "region":      random.choice(REGIONS),
            "city":        CITIES[i],
            "state":       STATES[i],
            "country":     "US",
            "zip_code":    random.randint(10000, 99999),
            "created_at":  start + timedelta(days=random.randint(0, 1000)),
        })
    return pd.DataFrame(rows)


def generate_stores(n: int = 1_000) -> pd.DataFrame:
    rows = []
    for k in range(n):
        i = random.randint(0, len(CITIES) - 1)
        rows.append({
            "store_id":   f"STORE_{k+1:04d}",
            "store_name": f"Store {k+1}",
            "city":       CITIES[i],
            "state":      STATES[i],
            "country":    "US",
            "zip_code":   random.randint(10000, 99999),
            "store_type": random.choice(STORE_TYPES),
        })
    return pd.DataFrame(rows)


def generate_products(n: int = 5_000) -> pd.DataFrame:
    rows = []
    for k in range(n):
        cat = random.choice(CATEGORIES)
        rows.append({
            "product_id":   f"PROD_{k+1:06d}",
            "product_name": f"{cat} Product {k+1}",
            "category":     cat,
            "unit_price":   round(random.uniform(5, 600), 2),
        })
    return pd.DataFrame(rows)


def generate_orders(customer_ids, store_ids, run_date: date, n: int = 500_000) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        rows.append({
            "order_id":     make_id("ORD"),
            "customer_id":  random.choice(customer_ids),
            "store_id":     random.choice(store_ids),
            "order_date":   run_date,
            "status":       random.choice(STATUSES),
            "channel":      random.choice(CHANNELS),
            "total_amount": round(random.uniform(20, 2000), 2),
        })
    return pd.DataFrame(rows)


def generate_order_items(order_ids, product_ids, run_date: date) -> pd.DataFrame:
    rows = []
    for order_id in order_ids:
        for _ in range(random.randint(1, 5)):
            qty   = random.randint(1, 10)
            price = round(random.uniform(5, 600), 2)
            disc  = round(random.choice([0.0, 0.05, 0.10, 0.15, 0.20]), 2)
            rows.append({
                "line_item_id": make_id("LI"),
                "order_id":     order_id,
                "product_id":   random.choice(product_ids),
                "quantity":     qty,
                "unit_price":   price,
                "discount":     disc,
                "line_total":   round(qty * price * (1 - disc), 2),
                "created_at":   run_date,
            })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(date.today()),
                        help="Run date YYYY-MM-DD (default: today)")
    parser.add_argument("--orders", type=int, default=500_000)
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date)
    y, m, d  = run_date.year, f"{run_date.month:02d}", f"{run_date.day:02d}"

    dims_dir  = "retail/dims"
    facts_dir = f"retail/facts/{y}/{m}/{d}"
    os.makedirs(dims_dir, exist_ok=True)
    os.makedirs(facts_dir, exist_ok=True)

    print("Generating customers...")
    customers = generate_customers()
    customers.to_parquet(f"{dims_dir}/customers.parquet", index=False)
    print(f"  {len(customers):,} customers → {dims_dir}/customers.parquet")

    print("Generating stores...")
    stores = generate_stores()
    stores.to_parquet(f"{dims_dir}/stores.parquet", index=False)
    print(f"  {len(stores):,} stores → {dims_dir}/stores.parquet")

    print("Generating products...")
    products = generate_products()
    products.to_parquet(f"{dims_dir}/products.parquet", index=False)
    print(f"  {len(products):,} products → {dims_dir}/products.parquet")

    print(f"Generating {args.orders:,} orders for {run_date}...")
    orders = generate_orders(
        customers["customer_id"].tolist(),
        stores["store_id"].tolist(),
        run_date, args.orders,
    )
    orders.to_parquet(f"{facts_dir}/orders.parquet", index=False)
    print(f"  {len(orders):,} orders → {facts_dir}/orders.parquet")

    print("Generating order line items (~3 per order on average)...")
    items = generate_order_items(
        orders["order_id"].tolist(),
        products["product_id"].tolist(),
        run_date,
    )
    items.to_parquet(f"{facts_dir}/order_line_items.parquet", index=False)
    print(f"  {len(items):,} line items → {facts_dir}/order_line_items.parquet")

    print("\nDone. Run the pipeline:")
    print(f"  make run-all")


if __name__ == "__main__":
    main()
