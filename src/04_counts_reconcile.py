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
# Injected by the job as {{job.run_id}} — serverless compute doesn't expose
# spark.databricks.job.runId, so the run id must arrive as a parameter.
dbutils.widgets.text("job_run_id", "interactive")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
job_run_id = dbutils.widgets.get("job_run_id")

ENTITIES = ["collectors", "shops", "cards", "orders", "order_items", "payments"]

# COMMAND ----------

# Same audit table the dbt on-run-end hook writes to — one query covers the
# whole pipeline's QC history across notebooks AND the dbt gate.
qc_log = f"{catalog}.{schema}.qc_log"
spark.sql(f"""
    create table if not exists {qc_log} (
        logged_at timestamp, run_date date, invocation_id string,
        source string, check_name string, table_name string,
        column_name string, status string, failures bigint,
        execution_time_s double, message string
    ) using delta
""")

def log_check(entity, status, failures, message):
    spark.sql(f"""
        insert into {qc_log}
        values (current_timestamp(), current_date(), '{job_run_id}',
                'reconcile', 'bronze_silver_count_reconcile', '{entity}', '',
                '{status}', {failures}, 0.0, '{message}')
    """)

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
        log_check(entity, "fail", bronze, "silver is empty")
    elif silver > bronze:
        problems.append(f"{entity}: silver ({silver}) > bronze ({bronze}) — rows invented")
        log_check(entity, "fail", silver - bronze, f"silver ({silver}) > bronze ({bronze})")
    elif ratio >= 1.5:
        suspicious.append(f"{entity}: bronze/silver ratio {ratio:.2f} — possible double load masked by dedupe")
        log_check(entity, "warn", bronze - silver, f"bronze/silver ratio {ratio:.2f} — possible masked double load")
    else:
        log_check(entity, "pass", 0, f"bronze={bronze} silver={silver} ratio={ratio:.2f}")

if suspicious:
    print("\n" + "!" * 70)
    for s in suspicious:
        print("WARNING:", s)
    print("!" * 70)

if problems:
    raise Exception("Reconcile FAILED: " + "; ".join(problems))
print("\nReconcile passed.")
