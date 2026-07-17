# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Counts reconcile (cross-layer QC)
# MAGIC Fan-in task: waits for every silver table, then compares bronze vs silver
# MAGIC row counts across ALL entities at once. Per-table tests can't see this —
# MAGIC a real production double-load was masked by dedupe and only exposed by
# MAGIC exactly this kind of cross-layer count comparison.
# MAGIC
# MAGIC Hard failures (silver empty, silver > bronze) stop the job. A suspicious
# MAGIC bronze/silver ratio prints a loud warning but lets the dbt gate deliver
# MAGIC the formal verdict.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse_demo")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

ENTITIES = ["collectors", "shops", "cards", "orders", "order_items", "payments"]

# COMMAND ----------

problems, suspicious = [], []
print(f"{'entity':<14} {'bronze':>8} {'silver':>8} {'ratio':>6}")
for entity in ENTITIES:
    bronze = spark.table(f"{catalog}.{schema}.bronze_{entity}").count()
    silver = spark.table(f"{catalog}.{schema}.silver_{entity}").count()
    ratio = bronze / silver if silver else float("inf")
    print(f"{entity:<14} {bronze:>8} {silver:>8} {ratio:>6.2f}")
    if silver == 0:
        problems.append(f"{entity}: silver is empty")
    elif silver > bronze:
        problems.append(f"{entity}: silver ({silver}) > bronze ({bronze}) — rows invented")
    elif ratio >= 1.5:
        suspicious.append(f"{entity}: bronze/silver ratio {ratio:.2f} — possible double load masked by dedupe")

if suspicious:
    print("\n" + "!" * 70)
    for s in suspicious:
        print("WARNING:", s)
    print("!" * 70)

if problems:
    raise Exception("Reconcile FAILED: " + "; ".join(problems))
print("\nReconcile passed.")
