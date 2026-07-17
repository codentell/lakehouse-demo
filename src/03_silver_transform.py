# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver transform (clean + type), one entity per task
# MAGIC Bronze → silver: cast types, drop exact reload duplicates. Parameterized
# MAGIC like the bronze notebook — the per-entity typing lives in one dict instead
# MAGIC of six notebooks.
# MAGIC
# MAGIC Note what silver does **not** do: it does not fix orphaned references or
# MAGIC null emails. Those flow through so the dbt gate has something real to
# MAGIC catch — cleaning and *verifying* are separate concerns.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse_demo")
dbutils.widgets.text("entity", "collectors")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
entity = dbutils.widgets.get("entity")

# One place to define each entity's key and typed columns.
SILVER = {
    "collectors": ("collector_id", [
        "cast(collector_id as bigint) as collector_id", "handle", "email", "state",
        "cast(signup_date as date) as signup_date"]),
    "shops": ("shop_id", [
        "cast(shop_id as bigint) as shop_id", "shop_name", "city", "state"]),
    "cards": ("card_id", [
        "cast(card_id as bigint) as card_id", "card_name", "set_name",
        "card_number", "rarity", "card_type",
        "cast(market_price as decimal(10,2)) as market_price"]),
    "orders": ("order_id", [
        "cast(order_id as bigint) as order_id", "cast(collector_id as bigint) as collector_id",
        "cast(shop_id as bigint) as shop_id", "cast(order_ts as timestamp) as order_ts",
        "cast(amount as decimal(10,2)) as amount", "status"]),
    "order_items": ("order_item_id", [
        "cast(order_item_id as bigint) as order_item_id", "cast(order_id as bigint) as order_id",
        "cast(card_id as bigint) as card_id", "cast(quantity as int) as quantity",
        "condition"]),
    "payments": ("payment_id", [
        "cast(payment_id as bigint) as payment_id", "cast(order_id as bigint) as order_id",
        "method", "cast(paid_amount as decimal(10,2)) as paid_amount"]),
}

key, columns = SILVER[entity]

# COMMAND ----------

# Keep one row per business key, preferring the most recent load.
#
# Teaching point: this silently *masks* a double load. Silver counts look
# right even when bronze loaded twice — which is exactly why the unique test
# must also exist at the bronze layer. Tests at one layer do not protect the
# others.
w = Window.partitionBy(key).orderBy(F.col("_loaded_at").desc())
silver = (
    spark.table(f"{catalog}.{schema}.bronze_{entity}")
    .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    .selectExpr(*columns)
)

table = f"{catalog}.{schema}.silver_{entity}"
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
print(f"{table}: {spark.table(table).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema enforcement demo (run live, any entity)
# MAGIC Delta rejects writes that don't match the table schema — the "warehouse
# MAGIC guarantee" half of the lakehouse pitch. Uncomment: it throws instead of
# MAGIC silently corrupting the table.

# COMMAND ----------

# from pyspark.sql import Row
# bad = spark.createDataFrame([Row(order_id="not-a-number", collector_id=1)])
# bad.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.silver_orders")
