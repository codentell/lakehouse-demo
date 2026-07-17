# Building a Lakehouse with Databricks + dbt
## A Proof-of-Concept Journey — from Free Edition to a Teachable Curriculum

**Presenter:** Drew Hoang
**Stack:** Databricks Free Edition · PySpark · dbt Core (`dbt-databricks`, Python models) · Databricks Asset Bundles (DABs) · GitHub Actions

> **Design choice for this demo: Spark-first.** Every transformation is written with the PySpark DataFrame API — including the dbt layer, which uses **dbt Python models** instead of SQL models. One language (Python) end-to-end: ingestion, transformation, and orchestration.

---

## 1. Why This Talk

Three questions, one journey:

1. **What is a Lakehouse**, and why do Databricks + dbt fit together so well?
2. **Can you build a real PoC for $0** — Free Edition, dbt Core, GitHub Actions — with production-shaped CI/CD?
3. **How do you turn that PoC into a course** graduate students can actually complete?

The answer to all three is the same demo, told three ways.

---

## 2. The Lakehouse in One Slide

A **Lakehouse** = the cheap, open storage of a data lake + the reliability and governance of a warehouse.

| Layer | What it gives you | Databricks piece |
|---|---|---|
| Storage | Open files (Parquet) with ACID transactions | **Delta Lake** |
| Governance | Catalogs, schemas, permissions, lineage | **Unity Catalog** |
| Compute | SQL + Python on the same data | **Serverless SQL Warehouse / Jobs** |
| Transformation | Version-controlled, tested SQL | **dbt** |
| Deployment | Infra + jobs as code | **Databricks Asset Bundles (DABs)** |

**The medallion architecture** is the mental model for the whole demo:

```
Bronze (raw, as-landed)  →  Silver (cleaned, typed, deduped)  →  Gold (business marts)
```

dbt's job is precisely the arrows: Bronze → Silver → Gold as versioned, tested code. In this demo the arrows are **PySpark DataFrames** (dbt Python models), not SQL — same DAG, same tests, same docs.

---

## 3. Where dbt Fits (and Where Databricks Ends)

- **Databricks owns**: storage (Delta), catalog (Unity Catalog), compute (serverless Spark + SQL warehouse), orchestration (Jobs), deployment (DABs).
- **dbt owns**: the transformation graph — `ref()` dependencies, tests, documentation, incremental logic.
- **The seam**: dbt connects to Databricks over an HTTP path. **SQL models** run on the SQL warehouse; **Python models** (our choice) are pushed to Databricks and executed as **PySpark** — dbt hands your `model()` function a live SparkSession-backed DataFrame and materializes the result as a Delta table in Unity Catalog.
- Databricks Jobs can then run dbt as a task, and DABs deploys the whole thing.

You get software-engineering discipline (Git, PRs, CI, tests) applied to Spark code — while keeping dbt's DAG, tests, and lineage.

---

## 4. The PoC Journey — Overview

Demo domain: a marketplace with **users, reviews, and transactions** (reusing the kiwi hackathon pipeline: review ingestion, staging jobs, dynamic-pricing proof). If you want zero setup, swap in the built-in `samples.nyctaxi.trips` dataset — everything else stays identical.

```
Step 0  Sign up: Databricks Free Edition
Step 1  Land raw data → Bronze (PySpark ingestion task, fan-out per source)
Step 2  Connect dbt Core to Databricks
Step 3  Model Silver + Gold as dbt PYTHON models (PySpark DataFrames),
        with referential-integrity tests + docs at every stage
Step 4  Wrap it all in a Databricks Asset Bundle — a multi-branch job DAG
        ending in a verify_integrity quality gate
Step 5  GitHub Actions: validate (dry-run) on PR, deploy on merge
Step 6  Run the deployed job; break it live; inspect lineage in Unity Catalog
```

---

## 5. Step 0 — Databricks Free Edition

- Sign up at **databricks.com/learn/free-edition** (replaced the old Community Edition).
- You get: a serverless workspace, **Unity Catalog**, a **serverless SQL warehouse**, Jobs/Workflows, notebooks — no cloud account or credit card required.
- Known Free Edition constraints to plan around:
  - **Serverless-only** compute (no classic clusters — fine for this demo).
  - Rate/size limits appropriate for learning, not production.
- Create the demo scaffolding from a Python notebook (everything in this demo stays in Python):

```python
# setup notebook — first cell of the course
for stmt in [
    "CREATE CATALOG IF NOT EXISTS lakehouse_demo",
    "CREATE SCHEMA IF NOT EXISTS lakehouse_demo.bronze",
    "CREATE SCHEMA IF NOT EXISTS lakehouse_demo.silver",
    "CREATE SCHEMA IF NOT EXISTS lakehouse_demo.gold",
]:
    spark.sql(stmt)
```

---

## 6. Step 1 — Bronze: Land the Raw Data

Keep ingestion deliberately simple — one Python task that writes Delta tables:

```python
# src/ingest_bronze.py — runs as a serverless job task, once per source
# (the job's for_each_task passes "reviews" | "users" | "transactions")
import sys
from pyspark.sql import functions as F

source = sys.argv[1]

raw = (spark.read.json(f"/Volumes/lakehouse_demo/bronze/landing/{source}/")
       .withColumn("_ingested_at", F.current_timestamp())
       .withColumn("_source", F.lit(source)))

raw.write.mode("append").saveAsTable(f"lakehouse_demo.bronze.{source}_raw")
```

Points to make on stage:
- Bronze is **append-only, schema-on-read, never edited** — reprocessing is always possible.
- The `_ingested_at` audit column is the habit you want students to form on day one.
- For the classroom version, a seed/generator notebook (like `transactions_generator.ipynb` from the hackathon project) makes everyone's data identical and reproducible.

---

## 7. Step 2 — Connect dbt Core to Databricks

Install and configure locally (or in GitHub Codespaces for a uniform student environment):

```bash
pip install dbt-databricks
dbt init lakehouse_demo_dbt
```

`~/.dbt/profiles.yml` — the entire connection is three values from the SQL warehouse's "Connection details" tab plus a token:

```yaml
lakehouse_demo_dbt:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: lakehouse_demo
      schema: silver
      host: <workspace>.cloud.databricks.com
      http_path: /sql/1.0/warehouses/<warehouse_id>
      token: "{{ env_var('DATABRICKS_TOKEN') }}"   # PAT from User Settings → Developer
      threads: 4
```

Verify the handshake — this is the demo's first applause moment:

```bash
export DATABRICKS_TOKEN=dapi...
dbt debug          # ✓ connection ok
```

---

## 8. Step 3 — Silver + Gold as dbt **Python** Models (PySpark)

Every model is a `.py` file with a `model(dbt, session)` function. dbt pushes it to Databricks, runs it as PySpark, and materializes the returned DataFrame as a Delta table. `dbt.source()` / `dbt.ref()` return **DataFrames**, so the dependency graph works exactly like SQL models.

Project layout — deliberately a **deep, fan-out/fan-in DAG**, not a straight line:

```
models/
├── sources.yml                     # declares bronze tables as dbt sources
├── silver/                         # 3-way fan-out from bronze
│   ├── stg_reviews.py
│   ├── stg_users.py
│   └── stg_transactions.py
├── intermediate/                   # cross-joins between branches
│   ├── int_user_sessions.py        #   stg_transactions → sessionization (window funcs)
│   ├── int_review_scores.py        #   stg_reviews × stg_users → weighted scores
│   └── int_fx_normalized.py        #   stg_transactions × fx_rates seed → USD amounts
└── gold/                           # fan-in: each mart reads 2–3 upstream models
    ├── fct_daily_revenue.py        #   incremental!
    ├── dim_user_activity.py
    └── ml_pricing_features.py      #   feature table for the dynamic-pricing model
```

`models/sources.yml` (YAML is still used for sources/tests/docs — the *models* are Python):

```yaml
sources:
  - name: bronze
    catalog: lakehouse_demo
    schema: bronze
    tables:
      - name: reviews_raw
      - name: users_raw
      - name: transactions_raw
```

`models/silver/stg_reviews.py` — clean, type, dedupe, in DataFrame API:

```python
from pyspark.sql import functions as F, Window

def model(dbt, session):
    dbt.config(materialized="table")

    raw = dbt.source("bronze", "reviews_raw")

    w = Window.partitionBy("review_id").orderBy(F.col("_ingested_at").desc())
    return (
        raw.withColumn("rn", F.row_number().over(w))
           .where("rn = 1")
           .select(
               "review_id",
               "user_id",
               F.col("rating").cast("int").alias("rating"),
               F.trim(F.lower("review_text")).alias("review_text"),
               F.col("created_at").cast("timestamp").alias("created_at"),
           )
    )
```

`models/intermediate/int_user_sessions.py` — the "real Spark" moment: sessionization with window functions, something painful in SQL and natural in PySpark:

```python
from pyspark.sql import functions as F, Window

SESSION_GAP_MIN = 30

def model(dbt, session):
    dbt.config(materialized="table")

    txn = dbt.ref("stg_transactions")

    w = Window.partitionBy("user_id").orderBy("transacted_at")
    gap = (F.unix_timestamp("transacted_at")
           - F.unix_timestamp(F.lag("transacted_at").over(w))) / 60

    return (
        txn.withColumn("new_session", (gap.isNull() | (gap > SESSION_GAP_MIN)).cast("int"))
           .withColumn("session_id", F.sum("new_session").over(w))
           .groupBy("user_id", "session_id")
           .agg(
               F.min("transacted_at").alias("session_start"),
               F.count("*").alias("events"),
               F.sum("amount").alias("session_revenue"),
           )
    )
```

`models/gold/fct_daily_revenue.py` — **incremental** Python model, fan-in of two branches:

```python
from pyspark.sql import functions as F

def model(dbt, session):
    dbt.config(materialized="incremental", incremental_strategy="merge",
               unique_key="revenue_date")

    txn = dbt.ref("int_fx_normalized")
    scores = dbt.ref("int_review_scores")

    if dbt.is_incremental:
        max_dt = session.table(f"{dbt.this}").agg(F.max("revenue_date")).first()[0]
        if max_dt:
            txn = txn.where(F.col("transacted_at") >= F.lit(max_dt))

    return (
        txn.join(scores, "user_id", "left")
           .groupBy(F.date_trunc("day", "transacted_at").alias("revenue_date"))
           .agg(
               F.countDistinct("transaction_id").alias("orders"),
               F.sum("amount_usd").alias("gross_revenue"),
               F.avg("weighted_rating").alias("avg_rating_that_day"),
           )
    )
```

Tests and docs come free in the same YAML — **identical for Python and SQL models**:

```yaml
models:
  # ── Silver: shape + referential integrity back to each other ──────────
  - name: stg_reviews
    columns:
      - name: review_id
        tests: [unique, not_null]
      - name: rating
        tests:
          - accepted_values:
              values: [1, 2, 3, 4, 5]
      - name: user_id
        tests:
          - not_null
          - relationships:              # RI: every review belongs to a real user
              to: ref('stg_users')
              field: user_id

  - name: stg_transactions
    columns:
      - name: transaction_id
        tests: [unique, not_null]
      - name: user_id
        tests:
          - relationships:              # RI: no orphan transactions
              to: ref('stg_users')
              field: user_id

  # ── Intermediate: RI must SURVIVE the joins/windows ────────────────────
  - name: int_user_sessions
    columns:
      - name: user_id
        tests:
          - relationships:
              to: ref('stg_users')
              field: user_id
      - name: session_revenue
        tests:
          - dbt_utils.accepted_range:   # quality: no negative sessions
              min_value: 0

  # ── Gold: business invariants ──────────────────────────────────────────
  - name: fct_daily_revenue
    columns:
      - name: revenue_date
        tests: [unique, not_null]
      - name: gross_revenue
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
```

**Singular tests** catch cross-stage conservation laws — assertions that no generic test expresses. A test is just a query that must return zero rows:

```sql
-- tests/assert_revenue_conserved_bronze_to_gold.sql
-- Total USD revenue in gold must equal bronze (±0.01 for fx rounding).
with bronze_total as (
    select sum(amount * fx.rate_to_usd) as v
    from {{ source('bronze', 'transactions_raw') }} t
    join {{ ref('fx_rates') }} fx using (currency)
),
gold_total as (
    select sum(gross_revenue) as v from {{ ref('fct_daily_revenue') }}
)
select * from bronze_total, gold_total
where abs(bronze_total.v - gold_total.v) > 0.01
```

Add row-count conservation the same way: `bronze.reviews_raw` distinct IDs = `stg_reviews` rows (dedup is the *only* allowed row loss, and it's measured).

Tag tests by stage so the verification job can report **per-stage** results:

```yaml
# dbt_project.yml
models:
  lakehouse_demo_dbt:
    silver:        {+tags: ["silver"]}
    intermediate:  {+tags: ["intermediate"]}
    gold:          {+tags: ["gold"]}
```

```bash
dbt build        # run models + tests in DAG order — a failed RI test STOPS downstream models
dbt test --select tag:silver          # verify one stage in isolation
dbt docs generate && dbt docs serve   # live lineage graph — second applause moment
```

Key mechanic to say out loud: `dbt build` interleaves tests *into* the DAG — if `stg_reviews` fails its `relationships` test, nothing downstream of it builds. Integrity failures halt the pipeline at the stage that broke, not at the end.

---

## 9. Step 4 — Databricks Asset Bundles (DABs)

DABs = **the whole project as code**: jobs, tasks, schedules, permissions, in one `databricks.yml`, deployable per-environment.

```
lakehouse-demo/
├── databricks.yml          # bundle definition
├── src/ingest_bronze.py    # bronze ingestion task
├── dbt/                    # the dbt project from Step 3
└── .github/workflows/ci.yml
```

`databricks.yml`:

```yaml
bundle:
  name: lakehouse_demo

targets:
  dev:
    mode: development       # resources get a per-user prefix — safe sandboxing
    default: true
    workspace:
      host: https://<workspace>.cloud.databricks.com
  prod:
    mode: production
    workspace:
      host: https://<workspace>.cloud.databricks.com

resources:
  jobs:
    lakehouse_pipeline:
      name: "lakehouse-demo-pipeline"
      tasks:
        # ── Stage 1: ingestion FAN-OUT — one parameterized task per source ──
        - task_key: ingest_each_source
          for_each_task:
            inputs: '["reviews", "users", "transactions"]'
            concurrency: 3
            task:
              task_key: ingest_bronze
              spark_python_task:
                python_file: src/ingest_bronze.py
                parameters: ["{{input}}"]
              environment_key: default

        # ── Stage 2: build the whole dbt DAG (models + inline tests) ────────
        - task_key: dbt_build
          depends_on: [{ task_key: ingest_each_source }]
          dbt_task:
            project_directory: dbt
            commands:
              - "dbt deps"
              - "dbt build"
            catalog: lakehouse_demo
            schema: silver

        # ── Stage 3: final verification — per-stage integrity & quality audit ─
        #   Re-runs ALL tests stage by stage, including the cross-stage
        #   conservation tests, and is the job's pass/fail gate.
        - task_key: verify_integrity
          depends_on: [{ task_key: dbt_build }]
          dbt_task:
            project_directory: dbt
            commands:
              - "dbt test --select tag:silver"
              - "dbt test --select tag:intermediate"
              - "dbt test --select tag:gold"
              - "dbt test --select test_type:singular"   # conservation laws
            catalog: lakehouse_demo
            schema: silver

        # ── Stage 4a: publish only if verification passed ────────────────────
        - task_key: refresh_dashboards
          depends_on: [{ task_key: verify_integrity }]
          run_if: ALL_SUCCESS
          spark_python_task:
            python_file: src/refresh_gold_views.py
          environment_key: default

        # ── Stage 4b: alert path if ANY upstream failed ──────────────────────
        - task_key: alert_on_failure
          depends_on:
            - { task_key: verify_integrity }
          run_if: AT_LEAST_ONE_FAILED
          spark_python_task:
            python_file: src/notify_failure.py
          environment_key: default

      environments:
        - environment_key: default
          spec:
            client: "2"
            dependencies: ["dbt-databricks"]
```

The job itself is now a real DAG, not a chain: a **for-each fan-out** over sources, a barrier into `dbt_build` (which internally runs the 9-model dbt DAG), a dedicated **`verify_integrity` gate** that re-audits every stage plus the cross-stage conservation tests, and **conditional branches** (`run_if: ALL_SUCCESS` / `AT_LEAST_ONE_FAILED`) for publish-vs-alert.

Two layers of DAG, deliberately:
- **dbt's DAG** (models + tests, resolved from `ref()`) handles *transformation* ordering.
- **The job's DAG** (tasks, `depends_on`, `run_if`, `for_each_task`) handles *operational* ordering — ingest, build, verify, publish/alert.

The three-command lifecycle:

```bash
databricks bundle validate      # the "dry run" — catches config errors before anything deploys
databricks bundle deploy -t dev # uploads code, creates/updates the job
databricks bundle run lakehouse_pipeline -t dev
```

Key teaching point: `validate` is a true **dry-run** — it type-checks the bundle, resolves variables, and renders the final job spec (`--output json` to inspect it) without touching the workspace. Students learn "plan before apply" the same way Terraform teaches it.

---

## 10. So — Can the Free Edition Do Complex DAG Processing?

**Yes, with eyes open.** The crucial insight: **DAG complexity is metadata, not compute.** A 9-model dbt graph with fan-out/fan-in, incremental merges, RI tests, and a 6-task job with for-each and conditional branches costs Databricks almost nothing to *orchestrate* — what's constrained on Free Edition is how much *data* flows through it and how *concurrently*.

What this demo exercises on Free Edition, all confirmed working:

| DAG feature | Where it appears in the demo |
|---|---|
| Multi-branch fan-out / fan-in | 3 bronze sources → 3 silver → 3 intermediate → 3 gold marts |
| Window functions & sessionization | `int_user_sessions` (lag/gap/cumulative-sum session IDs) |
| Incremental MERGE processing | `fct_daily_revenue` (`incremental_strategy="merge"`) |
| Referential integrity across stages | `relationships` tests silver → users, intermediate → silver |
| Cross-stage conservation audits | singular tests: revenue & row-count conserved bronze → gold |
| Parameterized task fan-out | `for_each_task` over the 3 sources |
| Conditional branching | `run_if: ALL_SUCCESS` publish vs `AT_LEAST_ONE_FAILED` alert |
| Stage-gated failure semantics | `dbt build` halts downstream of a failed test; `verify_integrity` gates publish |

Where you *will* feel Free Edition's ceiling — and the honest framing for students:

1. **Serverless-only, modest sizing** — wide shuffles over tens of GB will be slow or throttled; the DAG shape is unlimited, the data volume isn't. Keep demo data in the ~100 MB–1 GB range.
2. **Concurrency caps** — a `for_each_task` fanning out to 50 parallel tasks, or 30 students running jobs at 9:00 AM sharp, will queue. Design for width ≤ 5 and stagger classroom runs.
3. **Rate limits on jobs/API calls** — fine for scheduled pipelines, not for hammering the Jobs API in a loop.
4. **No classic clusters / custom Spark configs** — you tune with better code (partitioning, broadcast joins), not with cluster knobs. Pedagogically this is a feature.

The one-line answer for the slide: *"Free Edition limits how much data you push through the DAG — not how sophisticated the DAG is. Everything you'd learn about DAG design in production, you can learn here."*

---

## 11. Compute Breakdown — Same Pipeline, Different Cluster Settings

The pipeline's *logic* never changes; only the compute stanza in the bundle does. This is the section that turns "it works on Free Edition" into "here's how it runs anywhere."

### Who runs on what

| Pipeline stage | Free Edition (this demo) | Paid workspace — typical choice |
|---|---|---|
| `ingest_each_source` (PySpark) | Serverless jobs compute (`environment_key`) | Ephemeral **job cluster**, small autoscale |
| dbt **Python** models | Serverless (submitted via dbt-databricks) | Job cluster or serverless; Photon on |
| dbt **SQL** models / tests | Serverless SQL warehouse (2X-Small) | SQL warehouse sized S–L, auto-stop 10 min |
| `verify_integrity` (dbt test) | Same warehouse/serverless | Same — tests are cheap, don't oversize |
| Interactive dev (notebooks) | Serverless notebook compute | All-purpose cluster, **single-node** for dev |

Key vocabulary for students: **all-purpose clusters** (interactive, shared, expensive per-job), **job clusters** (created for one run, die after — the production default), **serverless** (Databricks manages everything; the only option on Free Edition), **SQL warehouses** (T-shirt-sized SQL engines dbt SQL models run on).

### The same job, three compute configurations

**A. Serverless (Free Edition — what the demo ships):** compute is declared as an `environments` spec; there are no knobs beyond dependencies. This is the entire stanza:

```yaml
environments:
  - environment_key: default
    spec:
      client: "2"
      dependencies: ["dbt-databricks"]
```

**B. Classic job cluster (paid, cost-optimized batch):** swap `environment_key` for `job_cluster_key` and define the cluster once, shared across tasks:

```yaml
job_clusters:
  - job_cluster_key: etl_cluster
    new_cluster:
      spark_version: 15.4.x-scala2.12
      node_type_id: m6gd.large            # AWS; use Standard_D4ds_v5 on Azure
      autoscale: { min_workers: 1, max_workers: 4 }
      runtime_engine: PHOTON              # vectorized engine, ~2-3x on wide scans
      aws_attributes:
        availability: SPOT_WITH_FALLBACK  # spot instances, ~60-70% cheaper
        first_on_demand: 1                # keep the driver on-demand
      spark_conf:
        spark.sql.shuffle.partitions: "64"   # tuned down from 200 for ~1 GB data

tasks:
  - task_key: ingest_bronze
    spark_python_task: { python_file: src/ingest_bronze.py }
    job_cluster_key: etl_cluster          # ← was environment_key on serverless
```

**C. Per-target overrides (the DABs payoff):** dev runs tiny, prod runs sized — one bundle, two shapes:

```yaml
targets:
  dev:
    mode: development
    resources:
      jobs:
        lakehouse_pipeline:
          job_clusters:
            - job_cluster_key: etl_cluster
              new_cluster:
                num_workers: 0            # single-node — driver does everything
                spark_conf: { "spark.databricks.cluster.profile": "singleNode" }
  prod:
    mode: production
    resources:
      jobs:
        lakehouse_pipeline:
          job_clusters:
            - job_cluster_key: etl_cluster
              new_cluster:
                autoscale: { min_workers: 2, max_workers: 8 }
```

### How each knob maps to what students observe

| Setting | What it changes | When it matters in *this* pipeline |
|---|---|---|
| `num_workers` / `autoscale` | Parallelism of shuffles & scans | Fan-out ingest + the sessionization window shuffle |
| Single-node (`num_workers: 0`) | Everything on the driver | Perfect for < 1 GB dev runs; falls over past a few GB |
| `runtime_engine: PHOTON` | Vectorized C++ execution | The gold aggregations and RI test scans speed up most |
| `spark.sql.shuffle.partitions` | Task count after shuffles | 200 default = 200 tiny tasks on 1 GB; 32–64 is right-sized |
| Spot + `first_on_demand` | Cost vs. eviction risk | Fine for idempotent bronze/silver; MERGE tasks prefer on-demand |
| Warehouse size (2X-S → L) | dbt SQL model & test latency | `verify_integrity` wall-clock; small is almost always enough |
| Warehouse auto-stop | Idle cost | Set 10 min; the #1 accidental-spend protector |

### dbt's side of the handshake

- **SQL models/tests** run wherever `http_path` points → resize by pointing at a bigger **warehouse** (a profiles/target change, not a code change).
- **Python models** run on jobs compute → on paid workspaces `dbt-databricks` lets you pin a cluster or stay serverless per model via model config (`submission_method`, `http_path` overrides). Demo default: everything serverless.

### The teaching frame

On Free Edition the compute section of the course is *conceptual* (you can't turn the knobs), and that's fine — the lab is: run the same `dbt build` twice on serverless, read the Spark UI's query plans, and *predict* which knobs would help where. Students who can articulate "this stage shuffles, so workers help; this stage scans, so Photon helps" have learned cluster sizing without spending a dollar.

---

## 12. Step 5 — The GitHub Workflow

The CI/CD story students should internalize: **PR = dry-run, merge = deploy.**

`.github/workflows/ci.yml`:

```yaml
name: lakehouse-ci

on:
  pull_request:
  push:
    branches: [main]

jobs:
  validate:                       # every PR: dry-run only
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - name: Bundle dry-run
        run: databricks bundle validate -t dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      - name: dbt compile check    # dbt's own "dry-run": compile SQL without executing
        run: |
          pip install dbt-databricks
          cd dbt && dbt deps && dbt compile --target ci

  deploy:                          # merge to main: deploy + run
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      - run: databricks bundle run lakehouse_pipeline -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

Two flavors of "dry-run," side by side:
- `databricks bundle validate` — is the **infrastructure** definition sound?
- `dbt compile` — does the **SQL** render and does the DAG resolve? (Add `dbt build --empty` or a `--select state:modified` slim CI later as an advanced topic.)

Secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) live in GitHub repo settings — nothing sensitive in code, ever.

---

## 13. Step 6 — Close the Loop

Live in the workspace, show:

1. The **job run graph**: the for-each ingest fan-out → `dbt_build` → `verify_integrity` → `refresh_dashboards`, all green (and the grayed-out `alert_on_failure` branch that never fired).
2. **Unity Catalog lineage**: click `gold.fct_daily_revenue` → see it traced back through intermediate and silver to bronze, automatically.
3. The **`verify_integrity` task output**: per-stage test results — silver ✓, intermediate ✓, gold ✓, conservation laws ✓ — this is the audit trail slide.
4. A one-cell dashboard query on the gold table — the "so what" for the business audience.

Then break it live: insert a review with a nonexistent `user_id` into bronze, rerun. Watch the `relationships` test fail, `dbt build` halt the affected branch, `verify_integrity` gate the publish, and `alert_on_failure` fire instead. **That's the whole system's value in one failed run.**

The PoC is complete: raw JSON → governed, tested, documented gold tables, deployed by a Git merge, verified stage-by-stage before anything publishes.

---

## 14. Pros & Cons — Why This System Is (and Isn't) Worth It

### Pros

1. **Failures are caught at the stage that caused them.** `dbt build` interleaves tests into the DAG, so a broken silver table never contaminates gold; the final `verify_integrity` gate means nothing publishes unverified. Compare: a cron-of-notebooks pipeline where you discover bad data when a stakeholder does.
2. **One language, one mental model.** Ingestion, transformation, and orchestration are all Python/PySpark; the DAG is the only "framework" concept. No SQL/Python context-switching mid-pipeline.
3. **Everything is text, so everything is reviewable.** Models, tests, job DAG, and CI are files in a PR. The transformation logic has a diff history, an approver, and a rollback path — SQL-in-a-dashboard has none of those.
4. **Two dry-run layers before anything touches data.** `databricks bundle validate` (infra) + `dbt compile` (DAG resolution) in CI mean the classic "deployed a typo" failure class is nearly eliminated.
5. **Lineage and docs are free byproducts**, not extra work: Unity Catalog traces column-level lineage automatically; `dbt docs` renders the graph from the `ref()` calls you already wrote.
6. **The dev→prod story is real**: `mode: development` sandboxes every student/engineer with prefixed resources; the same bundle deploys to prod unchanged.
7. **$0 to learn, production-shaped to keep.** Nothing in the design is Free-Edition-specific — the same repo deploys to a paid workspace by changing one host URL.

### Cons

1. **Two DAGs to reason about.** dbt's model graph *and* the job's task graph. Newcomers will ask "why is verification both inside `dbt build` and a separate task?" (Answer: inline tests stop bad data flowing; the final gate produces the per-stage audit and controls publish — but it *is* conceptual overhead.)
2. **dbt Python models are heavier than SQL models.** Each one executes as PySpark on serverless compute — slower to start and harder to debug than a SQL model on a warm warehouse. For a pure `select`-shaped transform, SQL is objectively simpler; we pay the Python tax for window-heavy logic and one-language consistency.
3. **Redundant test execution.** The verify stage re-runs tests that `dbt build` already ran — that's double compute, justified only because the gate + per-stage report has audit value. On big data you'd select only the cross-stage singular tests in the gate.
4. **Toolchain churn.** dbt-databricks, DABs schema, and serverless capabilities all evolve quickly; a course built on exact YAML shapes needs an annual refresh.
5. **Free Edition ceilings are real** (see §10): data volume, concurrency, no cluster tuning. Fine for learning; a production migration needs a paid workspace and real cluster sizing (§11) — budget that conversation.
6. **RI tests are detective, not preventive.** Delta/Unity Catalog doesn't *enforce* foreign keys on write; the `relationships` tests catch violations after materialization. The gate keeps bad data from *publishing*, but not from *existing* in silver — a nuance worth teaching explicitly.

### Net judgment

For anything beyond one notebook and one table, the system pays for itself the first time a test halts a bad deploy. The overhead is front-loaded (learn dbt + DABs once); the benefit is per-incident, forever.

---

## 15. Teaching This to Graduate Students

### Why this stack is ideal for a classroom
- **$0 and no cloud account**: Free Edition removes the AWS-billing-surprise failure mode entirely.
- **Everything is text**: dbt models, `databricks.yml`, and workflows are all reviewable in a PR — so grading = code review.
- **Industry-real**: this is the actual pattern (medallion + dbt + bundles + CI) they'll see at work, not a toy.

### 6-week module design (fits a half-semester or workshop series)

| Week | Topic | Deliverable |
|---|---|---|
| 1 | Lakehouse concepts, Free Edition signup, SQL warehouse, Delta basics | Query `samples.nyctaxi.trips`; create catalog/schemas |
| 2 | Bronze ingestion, medallion architecture, audit columns | Ingestion job writing a bronze table |
| 3 | dbt fundamentals: sources, `dbt.ref()`, **Python models** (PySpark) | `dbt debug` + 2 silver models running |
| 4 | Tests as contracts: RI (`relationships`), conservation tests, incremental models | Tested silver→intermediate→gold DAG, published docs site |
| 5 | DABs: `validate` / `deploy` / `run`, dev vs prod targets | Bundle deploying the full pipeline |
| 6 | GitHub Actions CI/CD, PR reviews, dry-runs | PR that passes validate/compile CI and deploys on merge |

### Pedagogy that works here
- **Fork-a-template**: give students a working repo (this demo) and have them extend it with a new domain — they learn the seams, not the boilerplate.
- **PR-based grading**: each week's deliverable is a pull request; CI green = baseline pass, code review = quality grade. The dry-run steps mean students get feedback *before* the instructor does.
- **Deliberate breakage labs**: hand out a bundle with a bad `http_path`, a failing `not_null` test, a circular `ref()` — debugging is where the learning compounds.
- **Capstone**: pick any public dataset → full medallion pipeline → dbt docs site + 5-minute demo of a merge-to-deploy. Rubric: correctness (tests pass), design (layer discipline), operations (CI works), communication (docs + lineage).

### Common student pitfalls (put these in the syllabus)
1. Tokens committed to Git → require `env_var()` in profiles from week 3, enable GitHub secret scanning.
2. Doing transformation in the ingestion notebook → "if it has business logic, it belongs in dbt."
3. Skipping `bundle validate` locally and burning CI cycles → make dry-run a muscle memory.
4. Free Edition serverless limits during class-wide runs → stagger job runs or pre-warm the warehouse.

---

## 16. Takeaways

1. **The Lakehouse is an architecture, not a product** — Delta + Unity Catalog + serverless Spark; dbt supplies the engineering discipline, and Python models let the whole thing stay PySpark end-to-end.
2. **The entire PoC costs nothing** — Free Edition + dbt Core + GitHub Actions is a complete, production-shaped stack, and it handles genuinely complex DAGs: the ceiling is data volume and concurrency, never DAG sophistication.
3. **Integrity is layered, not bolted on**: RI tests inside the build halt bad branches; the `verify_integrity` gate audits every stage plus conservation laws before anything publishes.
4. **Dry-runs are the pedagogy**: `bundle validate` and `dbt compile` teach plan-before-apply, and CI teaches feedback loops.
5. **The demo *is* the curriculum** — fork it, extend it, break it in labs, grade the PRs.

### Resources
- Databricks Free Edition: https://www.databricks.com/learn/free-edition
- Databricks Asset Bundles docs: https://docs.databricks.com/dev-tools/bundles/
- dbt-databricks adapter: https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup
- `databricks/setup-cli` GitHub Action: https://github.com/databricks/setup-cli
- Demo repo (this project): `<your GitHub URL here>`
