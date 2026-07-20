# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Manifest count (the gate at the front)
# MAGIC Before spending any compute on processing, verify the vendor actually
# MAGIC delivered every file and none are empty. In production this task compares
# MAGIC file counts and row counts against a vendor-provided manifest; a missing
# MAGIC file fails the job HERE, in seconds, instead of 3 hours into a transform.
# MAGIC Every downstream task depends on this one. Hello 

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
raw_root = f"/Volumes/{catalog}/{schema}/raw"

ENTITIES = ["collectors", "shops", "cards", "orders", "order_items", "payments"]

# COMMAND ----------

problems = []
print(f"{'entity':<14} {'files':>5} {'rows':>8}")
for entity in ENTITIES:
    try:
        files = dbutils.fs.ls(f"{raw_root}/{entity}/")
    except Exception:
        problems.append(f"{entity}: delivery folder missing")
        continue
    rows = spark.read.json(f"{raw_root}/{entity}/").count()
    print(f"{entity:<14} {len(files):>5} {rows:>8}")
    if rows == 0:
        problems.append(f"{entity}: delivered but empty")

if problems:
    raise Exception("Manifest check FAILED: " + "; ".join(problems))
print("\nManifest check passed — releasing downstream tasks.")