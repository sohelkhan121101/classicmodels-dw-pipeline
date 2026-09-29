# ClassicModels Data Pipeline

An end-to-end local data engineering pipeline built on the classic **`classicmodels`** MySQL sample database — built as a hands-on, interview-prep project covering the full medallion architecture, dimensional modeling, data quality, and schema evolution.

```
MySQL (classicmodels)  ──►  RAW  ──►  CURATED  ──►  GOLD
      (Docker)            (Parquet)   (Parquet)    (MySQL: classicmodels_dw)
                                          │
                                          └──► QUARANTINE (bad rows, Parquet)
```

## Architecture

| Layer | Format | Purpose |
|---|---|---|
| **Source** | MySQL (`classicmodels`, Dockerized) | The OLTP system of record |
| **Raw** | Parquet, partitioned by `ingestion_date` / `run_id` | Untouched extract, exactly as read from source |
| **Curated** | Parquet | Cleaned data — same grain as source, passed through the data quality engine |
| **Quarantine** | Parquet, partitioned by `quarantine_date` | Rows that failed a quality rule, tagged with a `quarantine_reason` |
| **Gold** | MySQL (`classicmodels_dw`, same Docker container) | Kimball star schema — dimensions (SCD1 + SCD2) and facts, ready for BI/analytics queries |

A bad row is **isolated, never fatal** — one broken row never breaks the pipeline.

### Star schema (Gold)

- **Dimensions:** `dim_customer` (SCD2), `dim_employee` (SCD1), `dim_office` (SCD1), `dim_product` (SCD1), `dim_productline` (SCD1), `dim_date` (conformed/role-playing), `dim_order_status` (SCD1)
- **Facts:** `fact_order_line` (grain: one row per order line item), `fact_payment` (grain: one row per payment)

### Data quality engine

Rules are config-driven, defined per table in `metadata/quality_rules.json` — no rule is hardcoded in Python. Supported rule types:

- `not_null`
- `min_value` / `max_value`
- `allowed_values`
- **Duplicate primary key** detection (built-in, always on — first occurrence is kept, the rest are quarantined)

Any row that fails one or more rules is routed to `data/quarantine/<table>/quarantine_date=YYYY-MM-DD/` with a semicolon-joined `quarantine_reason` column explaining exactly what broke.

### Incremental loads & watermarking

Tables configured with `"load_type": "incremental"` in `metadata/table_config.json` extract only rows newer than the last successful watermark (`metadata/watermark_state.json`). The watermark only advances on success — a failed run is automatically retried from the same point next time (self-healing).

### Schema evolution

Every extract compares the incoming MySQL schema against `metadata/schema_registry.json`. New/dropped/changed columns bump a schema version and log a warning — the pipeline keeps running rather than crashing on drift.

## Tech stack

- **PySpark** — extraction (JDBC), transformation, Parquet I/O
- **MySQL** (Docker) — both the source (`classicmodels`) and the Gold warehouse (`classicmodels_dw`)
- **pandas + SQLAlchemy + PyMySQL** — writing Gold dimensions/facts into MySQL
- **pytest** — unit tests for the quality-rule engine, constants, MySQL warehouse manager, and Gold transforms

## Project layout

```
classicmodels_pipeline/
├── config/
│   ├── secrets.yaml            # your real config — gitignored, never committed
│   └── secrets.yaml.example    # template — copy this to create secrets.yaml
├── metadata/
│   ├── table_config.json       # per-table load type, watermark column, primary key
│   ├── quality_rules.json      # per-table data quality rules
│   ├── watermark_state.json    # dynamic — regenerated, gitignored
│   └── schema_registry.json    # dynamic — regenerated, gitignored
├── src/
│   ├── extract/source_to_raw.py       # Source -> Raw (+ Archive)
│   ├── transform/raw_to_curated.py    # Raw -> Curated (+ Quarantine)
│   ├── transform/curated_to_gold.py   # Curated -> Gold (star schema, MySQL)
│   ├── warehouse/mysql_manager.py     # Gold warehouse (MySQL) DDL + read/write
│   └── utils/                          # config loader, constants, logger, quality rules, metadata manager, spark utils
├── tests/                       # pytest suite
├── run_pipeline.py               # runs all 3 stages end-to-end
├── reset_pipeline.py             # resets state/data/logs/gold for a fresh run
└── requirements.txt
```

## Setup

### 1. Prerequisites

- Docker running a MySQL container that already has the `classicmodels` sample database loaded
- Python 3.11 (a `.venv` is recommended)
- Java (required by PySpark) available on `PATH`

### 2. Install dependencies

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure secrets

```bash
cp config/secrets.yaml.example config/secrets.yaml
```

Edit `config/secrets.yaml` and fill in your real MySQL host/port/user/password for both the `mysql.source` block (the `classicmodels` source DB) and the `warehouse.mysql` block (the `classicmodels_dw` Gold DB — same container, different database, created automatically by the pipeline on first run).

> **`config/secrets.yaml` is gitignored and must never be committed.** Only `config/secrets.yaml.example` (with the `CHANGE_ME` placeholder) is version-controlled. See [Secrets & Git](#secrets--git) below.

### 4. Run

```bash
python run_pipeline.py
```

This runs all three stages in order: Source → Raw, Raw → Curated (+ Quarantine), Curated → Gold. Each stage logs progress to `logs/*.jsonl` and prints a `=== PIPELINE SUMMARY ===` at the end.

To run a single stage:

```bash
python -m src.extract.source_to_raw
python -m src.transform.raw_to_curated
python -m src.transform.curated_to_gold
```

### 5. Browse the results

Open DBeaver (or any MySQL client), connect to the same Docker MySQL container, and browse the `classicmodels_dw` database — it now holds the full Gold star schema.

### 6. Reset for a fresh run

```bash
python reset_pipeline.py                # full reset: state + raw/curated/quarantine + logs + TRUNCATE all Gold tables
python reset_pipeline.py --state-only   # only reset watermark/schema-registry JSON, keep data on disk and in Gold
```

`config/secrets.yaml` and `metadata/table_config.json` are static configuration and are never touched by a reset.

### 7. Run tests

```bash
pytest tests/
```

Tests that need a live MySQL connection auto-skip if no server is reachable.

## Secrets & Git

**`config/secrets.yaml` must never be committed** — it holds real database credentials. This repo's `.gitignore` already excludes it, along with all dynamic/regenerated files (`data/`, `logs/`, `metadata/watermark_state.json`, `metadata/schema_registry.json`, `.venv/`, `__pycache__/`, `.pytest_cache/`).

Only `config/secrets.yaml.example` (placeholder values) is tracked. Anyone cloning the repo copies it to `config/secrets.yaml` and fills in their own credentials, as in step 3 above.

Before your first push, it's worth double-checking nothing sensitive is staged:

```bash
git status
git diff --cached -- config/secrets.yaml   # should show nothing / file should not be staged
```

If `config/secrets.yaml` was ever accidentally committed in the past, `.gitignore` alone won't remove it from history — it would need `git rm --cached config/secrets.yaml` (to stop tracking it going forward) plus a history rewrite (e.g. `git filter-repo` or the BFG Repo-Cleaner) if it was ever pushed to a remote, followed by rotating the real password.

## Windows-specific notes

This project was developed and validated end-to-end on Windows. Two Windows-only fixes are already baked in:

1. **PySpark worker connection failures** (`Python worker failed to connect back`) — caused by Windows' "App execution alias" redirecting bare `python` calls to the Microsoft Store stub, which breaks PySpark's worker subprocess spawning. Fixed via `src/utils/spark_utils.configure_pyspark_python()`, which pins `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` to `sys.executable` before any `SparkSession` is built.
2. **Decimal vs. float mismatch** in `curated_to_gold.py` — without PyArrow installed, `toPandas()` on a Spark `DECIMAL` column returns Python `Decimal` objects instead of `float64`, which then can't be mixed with `float` values from `pandas.read_sql()`. Fixed with explicit `.astype(float)` casts on the affected columns. Installing `pyarrow` (`pip install pyarrow`) also resolves this and is recommended for performance.

## Known design decisions

- **Archiving is optional.** The `archive` path/key is not required in `secrets.yaml` — if omitted, `reset_pipeline.py` and the pipeline simply skip it rather than failing.
- **DDL and `date_key` (`%Y%m%d`) formatting are intentionally hardcoded** in the Gold schema — these are structural/schema-as-code contracts, not configuration values, and shouldn't be environment-driven.
- All other previously hardcoded values (SCD2 open-end date, lookback base date, date format, default log filename) are centralized in `src/utils/constants.py` and overridable via environment variables (e.g. `CLASSICMODELS_SCD2_OPEN_END_DATE`).




Table	        PK Type	                                      SCD	Reasoning
customers	    Single (customerNumber)	                      Type 2	creditLimit/sales_rep history matters
employees	    Single (employeeNumber)	                      Type 2	job_title/office history matters
offices	      Single (officeCode)	                          Type 1	Address rarely changes, no analytical dependency
products	    Single (productCode)	                        Type 2	buyPrice/MSRP history critical for margin
productlines	Single (productLine)	                        Type 1	Pure descriptive text, no analytical dependency
orders	      Single (orderNumber)	                        na	Fact-grain data, not a dimension
orderdetails	Composite (orderNumber+productCode)	          na	Fact-grain data, not a dimension
payments	C   omposite (customerNumber+checkNumber)         na	Fact-grain data, not a dimension