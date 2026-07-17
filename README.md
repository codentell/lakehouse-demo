# Lakehouse Demo — Databricks Free Edition + dbt

Companion demo for the "Lakehouse in the Real World" talk, and a **free
template students can fork**: a production-shaped pipeline (asset bundle,
dev/prod targets, git-driven deploys, complex task DAG, dbt quality gate)
that runs entirely on Databricks Free Edition at zero cost.

A small **Pokémon TCG marketplace** model (collectors / shops / cards /
orders / order_items / payments) with **deliberately dirty data** — real card
names, sets, and rarity-driven prices (Charizard carries the market, as
always) — processed through a production-shaped DAG:

```
generate_raw
    └── manifest_count                      (gate: fail in seconds, not hours)
          ├── bronze_collectors ─ silver_collectors ─┐
          ├── bronze_shops ────── silver_shops ──────┤
          ├── bronze_cards ────── silver_cards ──────┼─┐
          ├── bronze_orders ──────────────┐          │ │
          │                               ├─ silver_orders ──┬───────────────┐
          ├── bronze_order_items ─────────┼──────────────────┼─ silver_order_items
          └── bronze_payments ────────────┼──────────────────┴─ silver_payments
                                          │                          │
                                 counts_reconcile  ◄─── (all six silver tables)
                                          │
                              check_dbt_gate_enabled   (condition task)
                                          │ true
                                  dbt_quality_gate     (dbt build — THE GATE)
```

17 tasks, cross-entity FK dependencies (facts wait on their dimensions),
fan-out and fan-in — the same graph shape as a real production feed, in
miniature. On Free Edition only **5 tasks run concurrently** (account-wide),
so the 6-wide fan-out visibly queues in the run UI — in production the same
DAG runs 20-wide across multiple tuned clusters. There is no meaningful limit
on DAG *size* (the platform allows ~1,000 tasks per job); concurrency and the
daily serverless quota are the real constraints.

```
lakehouse-demo/
├── databricks.yml                  # DAB: variables + dev/prod targets
├── resources/
│   └── tcg_pipeline.job.yml        # the 17-task DAG + dbt gate
├── src/
│   ├── 00_generate_raw_data.py     # synthetic vendor files (with injected dirt)
│   ├── 01_manifest_count.py        # delivery gate: all files present & non-empty
│   ├── 02_bronze_load.py           # parameterized: files -> Delta append, per entity
│   ├── 03_silver_transform.py      # parameterized: dedupe + type, per entity
│   ├── 04_counts_reconcile.py      # cross-layer bronze-vs-silver count QC
│   └── 05_lakehouse_features.py    # time travel / restore / OPTIMIZE tour
├── init_scripts/                   # TEACHING EXHIBIT — can't run on Free Edition
│   ├── cluster_init.sh             # what dependency setup looks like on classic compute
│   └── requirements.txt            # exact pins = reproducible clusters
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example        # for running dbt locally
│   └── models/
│       ├── sources.yml             # unique / not_null / relationships + thresholds
│       ├── staging/                # stg_orders, stg_payments (views)
│       └── marts/                  # fct_daily_revenue (the payoff table)
└── .github/workflows/              # dev deploy on main, prod deploy on tag
```

## Setup (once, ~15 min)

1. Sign up: <https://www.databricks.com/learn/free-edition>
2. Install the CLI: `brew install databricks`
3. Authenticate: `databricks auth login --host https://<your-workspace>.cloud.databricks.com`
4. Deploy: `databricks bundle deploy -t dev` (from this folder)
5. Run: `databricks bundle run -t dev tcg_pipeline`

The job's final `dbt_quality_gate` task runs `dbt build` on the serverless SQL
warehouse (pinned `dbt-databricks==1.11.8`) — no local dbt needed. To also run
dbt from your laptop (faster iteration during the demo), follow the next section.

## Connecting dbt to Databricks (local setup)

The in-job dbt gate needs no configuration — Databricks injects the connection.
This section is for running `dbt build` from your own machine.

1. **Install the adapter** (pin it to match the job):

   ```bash
   pip install "dbt-databricks==1.11.8"
   ```

2. **Collect the three connection values** from your workspace:
   - **host** — your workspace URL without `https://`, e.g.
     `dbc-a1b2c3d4-e5f6.cloud.databricks.com` (it's in your browser address bar).
   - **http_path** — in the workspace UI: **SQL Warehouses → Serverless Starter
     Warehouse → Connection details** → copy *HTTP path*
     (looks like `/sql/1.0/warehouses/abc123def456`).
   - **auth** — easiest is OAuth (`auth_type: oauth`), which opens a browser to
     log in; no token to store. Alternatively create a PAT under
     **Settings → Developer → Access tokens** and use
     `token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"` — never paste a token
     into the file.

3. **Create your profile.** Copy `dbt/profiles.yml.example` to
   `~/.dbt/profiles.yml` and fill in the two placeholders:

   ```yaml
   lakehouse_demo:
     target: dev
     outputs:
       dev:
         type: databricks
         catalog: workspace            # Free Edition's default catalog
         schema: lakehouse_demo        # created by the pipeline
         host: <your-workspace>.cloud.databricks.com
         http_path: /sql/1.0/warehouses/<warehouse-id>
         auth_type: oauth
         threads: 4
   ```

   (Or keep the profile in-repo: `export DBT_PROFILES_DIR=./dbt` and name the
   file `dbt/profiles.yml` — it's gitignored so credentials can't be committed.)

4. **Verify and run** from the `dbt/` directory:

   ```bash
   cd dbt
   dbt debug     # checks connection + profile
   dbt build     # runs models + tests (needs bronze/silver tables to exist,
                 # so run the Databricks job at least once first)
   ```

## Local credentials via .env

For token-based auth (instead of `databricks auth login` OAuth), keep your
credentials in a `.env` file at the repo root — it's gitignored, so it can
never be committed.

1. Copy the template and fill it in:

   ```bash
   cp .env.example .env
   ```

   - `DATABRICKS_HOST` — your workspace URL **with** `https://`
   - `DATABRICKS_TOKEN` — a PAT from **Settings → Developer → Access tokens**
   - `DBT_DATABRICKS_TOKEN` — the same PAT; this is the name
     `dbt/profiles.yml.example` reads for local dbt runs

2. Load it into your shell before running CLI or dbt commands:

   ```bash
   set -a; source .env; set +a
   ```

   The Databricks CLI picks up `DATABRICKS_HOST`/`DATABRICKS_TOKEN`
   automatically, so `databricks bundle deploy -t dev` works with no auth
   profile. (Tools like [direnv](https://direnv.net) can auto-load `.env` on
   `cd` if you prefer.)

3. Rotate the token in the workspace UI if it ever leaks — a PAT is a
   password.

## What you need to add to make this pipeline work

Out of the box the repo is a template — these are the pieces *you* supply:

| Piece | Where | Why |
|---|---|---|
| A Databricks workspace | [Free Edition signup](https://www.databricks.com/learn/free-edition) | Everything runs here |
| CLI auth profile | `databricks auth login --host https://<your-workspace>.cloud.databricks.com` | `bundle deploy` reads the host from your auth profile — nothing is hardcoded in `databricks.yml` |
| SQL warehouse name match | `databricks.yml` → `warehouse_id` lookup | The bundle looks up a warehouse named **"Serverless Starter Warehouse"** (Free Edition default). Renamed yours? Update the lookup |
| Local dbt profile (optional) | `~/.dbt/profiles.yml` | Only for running dbt from your laptop — see the section above |
| GitHub secrets (CI/CD only) | Repo **Settings → Secrets and variables → Actions**: `DATABRICKS_HOST` (full URL with `https://`) and `DATABRICKS_TOKEN` (a PAT, or a service-principal token in real production) | Both workflows in `.github/workflows/` deploy with these; without them CI fails at the deploy step |
| Job notification email | `resources/tcg_pipeline.job.yml` → `email_notifications` | Points at the deploying user by default; set a real address/list if you want on-call-style alerts |

Deploy order that works from a fresh clone:

1. `databricks auth login ...` → 2. `databricks bundle deploy -t dev` →
3. `databricks bundle run -t dev tcg_pipeline` (creates schema + all tables,
   ends with the dbt gate) → 4. optionally set up local dbt and CI secrets.

## The demo script (maps to talk segments)

| Talk segment | What to show here |
|---|---|
| Open formats / lakehouse | Run `05_lakehouse_features.py`: DESCRIBE DETAIL, time travel, RESTORE, OPTIMIZE |
| DABs | `databricks.yml` targets + variables; `bundle deploy -t dev`, then the DAG in the UI |
| Complex pipelines | The run graph: fan-out, FK ordering, fan-in, condition task, 5-at-a-time queueing |
| dev vs prod | `bundle deploy -t prod`: env-prefixed name, unpaused schedule, own schema — same YAML |
| Init scripts | `init_scripts/cluster_init.sh` + the commented `job_clusters` exhibit in the job YAML — classic-compute dependency setup vs the serverless `environments` block above it (Free Edition deploys these files but can't execute them) |
| CI/CD | `.github/workflows/`: main → dev, semver tag → prod |
| QC / thresholds / RI | The double-load finale (below) + `models/sources.yml` line by line |
| On-call / alerts | `email_notifications` + commented `health` rules in the job YAML |

**The finale — the masked double load (5 min):**
1. Run the pipeline normally: all 17 tasks green. The gate passes with
   *warnings* (orders→collectors and order_items→cards orphans — known
   vendor dirt absorbed by thresholds).
2. Re-run with parameter `simulate_double_load = true`: every task still
   green. Bronze has 2× rows; silver dedupes it away so counts look fine.
   `counts_reconcile` prints a loud ratio warning but the formal verdict
   belongs to the gate —
3. — which FAILS the job on `unique bronze_orders.order_id`. Punchline: *the
   only thing that formally noticed was a 2-line test, at a layer where the
   damage was already masked downstream.*
4. Investigate with `DESCRIBE HISTORY` / time travel; fix with `RESTORE TABLE`.

**Live lever #2 (optional):** uncomment the orphan-payments block in
`00_generate_raw_data.py` — the zero-tolerance `relationships` test on
`payments.order_id` fails the gate on the next run. Payment with no order =
money you can't explain: some tests get thresholds, some get zero.

## What the dbt gate teaches (models/sources.yml)

- `unique` / `not_null` on every business key — catches the double load at
  the bronze layer, where dedupe can't mask it.
- `relationships` (referential integrity) with a **threshold system**:
  - orders→collectors: measured vendor baseline ~1% → `warn_if: >0`,
    `error_if: >150`. Warn on known dirt, error on regression.
  - order_items→cards: baseline ~15 → `warn_if: >0`, `error_if: >25`.
  - payments→orders and orders→shops: zero tolerance, plain `error`.
  - Rule: **measure the baseline before picking numbers.**
- `severity: warn` on collector emails — known dirt gets logged, not paged.
- Error-severity failure fails the task → fails the job → downstream never
  sees bad data. That's the gate.

## Free Edition limits that shaped this template

- **Serverless only, no custom compute** — no `job_clusters`, no init scripts.
  The dbt gate uses the native serverless `dbt_task` + an `environments`
  dependency spec instead of a cluster init script installing dbt.
- **Max 5 concurrent job tasks (account-wide)** — big DAGs are fine (the
  platform allows ~1,000 tasks per job); wide fan-outs queue rather than
  running in parallel.
- **Daily serverless usage quota** — exceed it and compute shuts off for the
  rest of the day. This demo's data is deliberately tiny; don't loop the job.
- **One workspace per account** — the `prod` target is the same workspace with
  different variables; in production it's a separate workspace deployed only by CI.
- **One SQL warehouse, 2X-Small** — plenty for this data volume.
- **Outbound internet limited to trusted domains** — PyPI works; arbitrary APIs may not.
- No SSO/SCIM/account API; non-commercial use only.

Details: <https://docs.databricks.com/aws/en/getting-started/free-edition-limitations>
