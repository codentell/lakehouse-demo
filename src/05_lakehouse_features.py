# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Lakehouse features tour (run interactively, not part of the job)
# MAGIC Five-minute walk through the Delta features that justify the "open formats"
# MAGIC segment of the talk: it's just Parquet + a transaction log, and that log
# MAGIC buys you history, time travel, and restore.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
orders = f"{catalog}.{schema}.bronze_orders"

# COMMAND ----------

# MAGIC %md ### 1. A Delta table is just files — no proprietary format

# COMMAND ----------

# Every write in DESCRIBE HISTORY maps to Parquet files + a JSON commit in _delta_log.
# Any engine that speaks Delta (Spark, DuckDB, pandas, Trino, Polars) can read this.
display(spark.sql(f"DESCRIBE DETAIL {orders}"))

# COMMAND ----------

# MAGIC %md ### 2. Time travel — query the past

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {orders}")
display(history.select("version", "timestamp", "operation", "operationMetrics.numOutputRows"))

# COMMAND ----------

# Compare current count vs the very first version of the table.
current = spark.table(orders).count()
v0 = spark.sql(f"SELECT count(*) AS n FROM {orders} VERSION AS OF 0").first().n
print(f"now: {current} rows   |   version 0: {v0} rows")
# If you ran the double-load demo, the difference is visible RIGHT HERE —
# time travel is how you answer "when did this break?"

# COMMAND ----------

# MAGIC %md ### 3. Restore — the undo button

# COMMAND ----------

# The fix for a bad load isn't re-processing everything; it's one command:
# spark.sql(f"RESTORE TABLE {orders} TO VERSION AS OF 0")
# (Leave commented unless you want to actually roll back mid-demo.)

# COMMAND ----------

# MAGIC %md ### 4. The small-file problem and OPTIMIZE
# MAGIC The lakehouse con from the pros/cons slide: many small writes = many small
# MAGIC files = slow reads. `OPTIMIZE` compacts them. In production this runs as a
# MAGIC scheduled maintenance job — storage that maintains itself is not free.

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {orders}"))
