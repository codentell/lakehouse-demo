# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Generate raw data
# MAGIC Simulates a vendor dropping files into object storage: 6 entities written
# MAGIC as JSON to a Unity Catalog **Volume** (the lakehouse answer to "an S3
# MAGIC landing bucket"). Volumes are small on purpose — this template is sized
# MAGIC to run on Free Edition's daily quota many times over.
# MAGIC
# MAGIC The data is deliberately dirty, because vendor data always is:
# MAGIC - ~2% of customers have a **null email** (severity: warn demo)
# MAGIC - ~1% of orders point at a **customer that doesn't exist** (RI threshold demo)
# MAGIC - 15 order_items point at **products that don't exist** (RI threshold demo)
# MAGIC - (opt-in, commented out below) payments pointing at **orders that don't
# MAGIC   exist** — uncomment to trip the zero-tolerance RI test live

# COMMAND ----------

import json
import random
from datetime import datetime, timedelta

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")
raw_root = f"/Volumes/{catalog}/{schema}/raw"

random.seed(42)  # same data every run — demos should be reproducible

# COMMAND ----------

N_CUSTOMERS, N_STORES, N_PRODUCTS, N_ORDERS = 1_000, 50, 500, 10_000

STATES = ["CA", "NY", "TX", "FL", "WA", "IL", "MA", "CO"]
CITIES = ["Springfield", "Riverton", "Lakeside", "Fairview", "Georgetown"]
CATEGORIES = ["electronics", "home", "apparel", "grocery", "toys"]
STATUSES = ["completed", "completed", "completed", "shipped", "returned", "cancelled"]
METHODS = ["card", "card", "card", "paypal", "gift_card"]

base_date = datetime(2025, 1, 1)

customers = [{
    "customer_id": cid,
    "name": f"Customer {cid}",
    # dirt #1: ~2% null emails — real vendors do this constantly
    "email": None if random.random() < 0.02 else f"customer{cid}@example.com",
    "state": random.choice(STATES),
    "signup_date": (base_date - timedelta(days=random.randint(0, 900))).strftime("%Y-%m-%d"),
} for cid in range(1, N_CUSTOMERS + 1)]

stores = [{
    "store_id": sid,
    "city": random.choice(CITIES),
    "state": random.choice(STATES),
} for sid in range(1, N_STORES + 1)]

products = [{
    "product_id": pid,
    "product_name": f"Product {pid}",
    "category": random.choice(CATEGORIES),
    "price": round(random.uniform(3, 300), 2),
} for pid in range(1, N_PRODUCTS + 1)]

orders = []
for oid in range(1, N_ORDERS + 1):
    # dirt #2: ~1% of orders reference a customer that was never sent to us
    customer_id = random.randint(9000, 9999) if random.random() < 0.01 else random.randint(1, N_CUSTOMERS)
    orders.append({
        "order_id": oid,
        "customer_id": customer_id,
        "store_id": random.randint(1, N_STORES),
        "order_ts": (base_date + timedelta(minutes=random.randint(0, 260_000))).strftime("%Y-%m-%d %H:%M:%S"),
        "amount": round(random.uniform(5, 400), 2),
        "status": random.choice(STATUSES),
    })

order_items, iid = [], 1
for o in orders:
    for _ in range(random.randint(1, 3)):
        order_items.append({
            "order_item_id": iid,
            "order_id": o["order_id"],
            "product_id": random.randint(1, N_PRODUCTS),
            "quantity": random.randint(1, 4),
        })
        iid += 1
# dirt #4: 15 items referencing products that don't exist
for _ in range(15):
    order_items.append({
        "order_item_id": iid,
        "order_id": random.randint(1, N_ORDERS),
        "product_id": random.randint(8000, 8999),
        "quantity": 1,
    })
    iid += 1

payments, pid = [], 1
for o in orders:
    if o["status"] in ("completed", "shipped", "returned"):
        payments.append({
            "payment_id": pid,
            "order_id": o["order_id"],
            "method": random.choice(METHODS),
            "paid_amount": o["amount"],
        })
        pid += 1
# LIVE LEVER #2: payments for orders that don't exist — money we can't
# explain. The dbt test on payments.order_id is zero-tolerance (no threshold),
# so uncommenting this fails the gate on the very next run.
# for _ in range(30):
#     payments.append({
#         "payment_id": pid,
#         "order_id": random.randint(50_000, 59_999),
#         "method": random.choice(METHODS),
#         "paid_amount": round(random.uniform(5, 400), 2),
#     })
#     pid += 1

# COMMAND ----------

def write_jsonl(name, rows):
    path = f"{raw_root}/{name}/{name}.json"
    dbutils.fs.mkdirs(f"{raw_root}/{name}")
    dbutils.fs.put(path, "\n".join(json.dumps(r) for r in rows), overwrite=True)
    print(f"wrote {len(rows):>6} rows -> {path}")

for name, rows in [
    ("customers", customers), ("stores", stores), ("products", products),
    ("orders", orders), ("order_items", order_items), ("payments", payments),
]:
    write_jsonl(name, rows)
