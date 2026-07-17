# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze load (files → Delta), one entity per task
# MAGIC This notebook is **parameterized**: the job runs it six times in parallel,
# MAGIC once per entity, each as its own task in the DAG. One reusable notebook +
# MAGIC N parameterized tasks is how production pipelines get wide graphs without
# MAGIC N copies of the code.
# MAGIC
# MAGIC Bronze is append-only and untyped on purpose: land it exactly as
# MAGIC received, fix it in silver.
# MAGIC
# MAGIC ### The live demo moment
# MAGIC Set job parameter `simulate_double_load = true` and re-run. Row counts
# MAGIC double, the pipeline stays **green**, nothing complains — until the dbt
# MAGIC gate's `unique` test runs. This reproduces a real incident class: a
# MAGIC retried/replayed load nobody notices because "the job succeeded."

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse_demo")
dbutils.widgets.text("entity", "collectors")
dbutils.widgets.text("simulate_double_load", "false")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
entity = dbutils.widgets.get("entity")
double_load = dbutils.widgets.get("simulate_double_load").lower() == "true"

raw_root = f"/Volumes/{catalog}/{schema}/raw"
load_count = 2 if double_load else 1
table = f"{catalog}.{schema}.bronze_{entity}"

# COMMAND ----------

df = (
    spark.read.json(f"{raw_root}/{entity}/")
    .withColumn("_loaded_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# Idempotent for the demo: start fresh, then append N times.
spark.sql(f"DROP TABLE IF EXISTS {table}")
for _ in range(load_count):
    df.write.format("delta").mode("append").saveAsTable(table)

print(f"{table}: {spark.table(table).count()} rows (loads: {load_count})")
if double_load:
    print("*** Double load simulated. This task is GREEN. The dbt gate will not be. ***")

# COMMAND ----------

# Delta gives you an audit log for free — every write is a transaction.
# This is how you'd investigate the double load after the test catches it.
display(spark.sql(f"DESCRIBE HISTORY {table}"))
