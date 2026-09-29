"""
raw_to_curated.py
------------------
Layer: RAW -> CURATED + QUARANTINE for bad rows

Responsibilities:
  1. Read the LATEST raw partition for a table (today's run_id).
  2. Apply data-quality rules from metadata/quality_rules.json, PLUS a
     built-in duplicate-primary-key check - see src/utils/quality_rules.py.
  3. Split: valid rows -> CURATED zone, invalid rows -> QUARANTINE zone
     (tagged with a "quarantine_reason" saying exactly what broke).
     A bad row must NEVER break the pipeline - it's isolated, not fatal.
  4. Write curated output as Parquet, overwriting the "current" curated
     snapshot for that table. Duplicates are already resolved by the
     quality-rule engine (first occurrence kept, rest quarantined), so
     there's no separate dropDuplicates() step here anymore.
"""

import sys
import os
from datetime import datetime, timezone
import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession

from src.utils.config_loader import load_secrets, load_table_config, get_active_tables, resolve_path
from src.utils.logger import get_logger, ActivityLogger
from src.utils.metadata_manager import MetadataManager
from src.utils.quality_rules import load_quality_rules, apply_quality_rules
from src.utils.constants import DATE_FORMAT
from src.utils.spark_utils import configure_pyspark_python

def get_latest_raw_path(raw_base_path: str, table_name: str) -> str:
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
                   table_name: str, cfg: dict, quality_rules: dict):

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
        rules = quality_rules.get(table_name, [])
        valid_df, invalid_df = apply_quality_rules(df, rules, primary_key_cols)

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        curated_path = os.path.join(curated_base, table_name)
        valid_df.write.mode("overwrite").parquet(curated_path)

        if invalid_count > 0:
            quarantine_date = start_time.strftime(DATE_FORMAT)
            quarantine_path = os.path.join(quarantine_base, table_name, f"quarantine_date={quarantine_date}")
            invalid_df.write.mode("append").parquet(quarantine_path)

            # reason breakdown for the log line, e.g. {"not_null[email]": 3, "duplicate_primary_key": 1}
            reason_counts = (
                invalid_df.selectExpr("explode(split(quarantine_reason, '; ')) as reason")
                .groupBy("reason").count().collect()
            )
            reason_summary = {r["reason"]: r["count"] for r in reason_counts}

            alog.warning(activity, f"{invalid_count} invalid row(s) quarantined",
                         extra={"quarantine_path": quarantine_path, "reason_breakdown": reason_summary})

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
    quality_rules = load_quality_rules(resolve_path(secrets["metadata"]["quality_rules_file"]))
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
    configure_pyspark_python()
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
            process_table(spark, secrets, meta, alog, table_name, cfg, quality_rules)
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
