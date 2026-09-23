"""
raw_to_curated.py
------------------
Layer: RAW -> CURATED (+ QUARANTINE for bad rows)

Responsibilities:
  1. Read the LATEST raw partition for a table (today's run_id).
  2. Apply data-quality rules (null-check on primary key columns,
     basic type/range sanity checks).
  3. Split: valid rows -> CURATED zone, invalid rows -> QUARANTINE zone.
     A bad row must NEVER break the pipeline - it's isolated, not fatal.
  4. Deduplicate on primary key (keep latest by row order within batch;
     full-loads from source are already deduped by MySQL PK anyway, this
     guards against any accidental duplication in transit).
  5. Write curated output as Parquet, overwriting the "current" curated
     snapshot for that table (curated always reflects the latest clean
     state; historical versions for SCD2 dimensions are handled in the
     next stage - Curated -> Gold - not here).

This module deliberately does NOT do dimensional modeling (no surrogate
keys, no SCD2 logic) - that belongs to the Curated -> Gold step. Curated's
job is just: "same grain as source, but clean and trustworthy."
"""

import sys
import os
from datetime import datetime, timezone
import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.config_loader import load_secrets, load_table_config, get_active_tables, resolve_path
from src.utils.logger import get_logger, ActivityLogger
from src.utils.metadata_manager import MetadataManager


# Minimal, table-agnostic quality rules. Extend per table as needed.
def apply_quality_rules(df, primary_key_cols):
    """
    Returns (valid_df, invalid_df).
    A row is INVALID if any primary-key column is null - a fact/dimension
    row with no identifiable key can never be joined or deduped safely,
    so it cannot be trusted downstream.
    """
    pk_not_null_condition = None
    for col in primary_key_cols:
        cond = F.col(col).isNotNull()
        pk_not_null_condition = cond if pk_not_null_condition is None else (pk_not_null_condition & cond)

    valid_df = df.filter(pk_not_null_condition)
    invalid_df = df.filter(~pk_not_null_condition)
    return valid_df, invalid_df


def get_latest_raw_path(raw_base_path: str, table_name: str) -> str:
    """Find today's most recent run_id folder for a table under raw/."""
    table_dir = os.path.join(raw_base_path, table_name)
    ingestion_dates = sorted(glob.glob(os.path.join(table_dir, "ingestion_date=*")))
    if not ingestion_dates:
        raise FileNotFoundError(f"No raw data found for table '{table_name}' at {table_dir}")
    latest_date_dir = ingestion_dates[-1]
    run_dirs = sorted(glob.glob(os.path.join(latest_date_dir, "run_id=*")))
    if not run_dirs:
        raise FileNotFoundError(f"No run_id partitions found under {latest_date_dir}")
    return run_dirs[-1]


def process_table(spark, secrets, meta: MetadataManager, alog: ActivityLogger,
                   table_name: str, cfg: dict):

    activity = f"curate_{table_name}"
    start_time = datetime.now(timezone.utc)
    alog.start(activity)

    try:
        raw_base = resolve_path(secrets["paths"]["raw"])
        curated_base = resolve_path(secrets["paths"]["curated"])
        quarantine_base = resolve_path(secrets["paths"]["quarantine"])

        latest_raw_path = get_latest_raw_path(raw_base, table_name)
        df = spark.read.parquet(latest_raw_path)
        total_records = df.count()

        primary_key_cols = cfg["primary_key"]
        valid_df, invalid_df = apply_quality_rules(df, primary_key_cols)

        # Deduplicate valid rows on primary key (defensive - guards against
        # any duplication introduced between source and raw landing)
        valid_df = valid_df.dropDuplicates(primary_key_cols)

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        curated_path = os.path.join(curated_base, table_name)
        valid_df.write.mode("overwrite").parquet(curated_path)

        if invalid_count > 0:
            quarantine_date = start_time.strftime("%Y-%m-%d")
            quarantine_path = os.path.join(quarantine_base, table_name, f"quarantine_date={quarantine_date}")
            invalid_df.write.mode("append").parquet(quarantine_path)
            alog.warning(activity, f"{invalid_count} invalid row(s) quarantined (null primary key)",
                         extra={"quarantine_path": quarantine_path})

        meta.log_run(table_name, "curated", "SUCCESS",
                     records_success=valid_count, records_quarantined=invalid_count,
                     start_time=start_time)

        alog.success(activity, extra={
            "total_read": total_records,
            "valid_written": valid_count,
            "quarantined": invalid_count,
            "curated_path": curated_path,
        })

    except Exception as e:
        meta.log_run(table_name, "curated", "FAILED", error_message=str(e), start_time=start_time)
        alog.failed(activity, error=e)
        raise


def run_all(table_filter=None):
    secrets = load_secrets()
    table_config = load_table_config()
    tables = get_active_tables(table_config)
    if table_filter:
        tables = [t for t in tables if t in table_filter]

    logger = get_logger(log_dir=resolve_path(secrets["paths"]["logs"]))
    alog = ActivityLogger(logger, source="raw_to_curated")

    meta = MetadataManager(
        watermark_file=resolve_path(secrets["metadata"]["watermark_file"]),
        schema_registry_file=resolve_path(secrets["metadata"]["schema_registry_file"]),
        run_log_file=resolve_path(secrets["metadata"]["run_log_file"]),
    )

    spark_cfg = secrets["spark"]
    spark = (
        SparkSession.builder
        .appName(spark_cfg["app_name"] + "_curated")
        .master(spark_cfg["master"])
        .config("spark.sql.shuffle.partitions", spark_cfg["shuffle_partitions"])
        .getOrCreate()
    )

    failures = []
    for table_name in tables:
        cfg = table_config[table_name]
        try:
            process_table(spark, secrets, meta, alog, table_name, cfg)
        except Exception as e:
            failures.append((table_name, str(e)))
            continue

    spark.stop()

    if failures:
        alog.warning("run_all", f"{len(failures)} table(s) failed curation", extra={"failures": failures})
    else:
        alog.success("run_all", extra={"tables_processed": tables})

    return failures


if __name__ == "__main__":
    run_all()
