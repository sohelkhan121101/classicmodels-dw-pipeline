"""
source_to_raw.py
-----------------
Layer: SOURCE (MySQL) -> RAW 

Responsibilities:

  1. Read table_config.json to decide FULL vs INCREMENTAL load per table.
  2. For incremental tables, read the last watermark from metadata_manager
     and build a bounded query (with lookback window where configured).
  3. Extract via PySpark JDBC.
  4. Detect schema evolution vs the schema_registry (never breaks the run).
  5. Write the batch to RAW (latest run's landing zone) and ARCHIVE
     (permanent historical copy) as partitioned Parquet.
  6. On SUCCESS: advance the watermark and write a SUCCESS run-log row.
  7. On FAILURE: leave the watermark untouched, write a FAILED run-log row,
     and re-raise - this is what makes the pipeline "self-healing": the
     next scheduled run will automatically retry from the same watermark,
     no human needs to reset anything.

Run standalone for local testing:
    python -m src.extract.source_to_raw
"""

import sys
import os
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession

from src.utils.config_loader import load_secrets, load_table_config, get_active_tables, resolve_path
from src.utils.logger import get_logger, ActivityLogger
from src.utils.metadata_manager import MetadataManager


# build_spark_session 
def build_spark_session(secrets: dict) -> SparkSession:
    spark_cfg = secrets["spark"]
    return (
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        .master(spark_cfg["master"])
        .config("spark.jars.packages", secrets["mysql"]["source"]["jdbc_jar_package"])
        .config("spark.sql.shuffle.partitions", spark_cfg["shuffle_partitions"])
        .getOrCreate()
    )


def build_jdbc_url(mysql_cfg: dict) -> str:
    return f"jdbc:mysql://{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}"


def build_extract_query(table_name: str, cfg: dict, last_watermark) -> str:
    if cfg["load_type"] == "full":
        return f"(SELECT * FROM {table_name}) AS t"

    watermark_col = cfg["watermark_column"]
    lookback_days = cfg.get("lookback_days", 0)

    if lookback_days and lookback_days > 0:
        # date-based lookback window - re-pulls recent days to catch
        # late updates (e.g. orders.status / orders.shippedDate changing
        # after order creation), since source has no updated_at column.
        base_date = last_watermark if last_watermark not in (None, "1900-01-01") else "1900-01-01"
        try:
            lookback_date = (datetime.strptime(base_date, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        except ValueError:
            lookback_date = base_date
        return f"(SELECT * FROM {table_name} WHERE {watermark_col} >= '{lookback_date}') AS t"

    # simple monotonic incremental (e.g. orderdetails.orderNumber)
    return f"(SELECT * FROM {table_name} WHERE {watermark_col} > '{last_watermark}') AS t"


def check_schema_evolution(meta: MetadataManager, alog: ActivityLogger, table_name: str, spark_df) -> None:
    """
    Compares incoming DataFrame schema against the last known schema in the
    registry. NEVER raises - schema drift is logged as a WARNING and the
    pipeline continues, per the requirement "invalid/changed data should not
    break the pipeline".
    """
    incoming_schema = {f.name: str(f.dataType) for f in spark_df.schema.fields}
    known = meta.get_known_schema(table_name)

    if known is None:
        meta.update_schema(table_name, incoming_schema, version=1)
        alog.warning(table_name, f"First-time schema registered (v1) with {len(incoming_schema)} columns",
                     extra={"columns": list(incoming_schema.keys())})
        return

    known_schema = known["schema"]
    new_cols = set(incoming_schema) - set(known_schema)
    dropped_cols = set(known_schema) - set(incoming_schema)
    changed_cols = {c for c in incoming_schema if c in known_schema and incoming_schema[c] != known_schema[c]}

    if new_cols or dropped_cols or changed_cols:
        new_version = known["version"] + 1
        meta.update_schema(table_name, incoming_schema, version=new_version)
        alog.warning(
            table_name,
            f"Schema drift detected, registered as v{new_version}",
            extra={
                "new_columns": list(new_cols),
                "dropped_columns": list(dropped_cols),
                "changed_type_columns": list(changed_cols),
            },
        )


def extract_table(spark, secrets, meta: MetadataManager, alog: ActivityLogger,
                   table_name: str, cfg: dict, run_id: str):

    activity = f"extract_{table_name}"
    start_time = datetime.now(timezone.utc)
    alog.start(activity, extra={"load_type": cfg["load_type"]})

    mysql_cfg = secrets["mysql"]["source"]
    jdbc_url = build_jdbc_url(mysql_cfg)
    jdbc_props = {"user": mysql_cfg["user"], "password": mysql_cfg["password"], "driver": mysql_cfg["jdbc_driver"]}

    try:
        last_watermark = meta.get_watermark(table_name) if cfg["load_type"] == "incremental" else None
        query = build_extract_query(table_name, cfg, last_watermark)

        df = spark.read.jdbc(jdbc_url, query, properties=jdbc_props)
        record_count = df.count()

        check_schema_evolution(meta, alog, table_name, df)

        ingestion_date = start_time.strftime("%Y-%m-%d")
        raw_path = os.path.join(resolve_path(secrets["paths"]["raw"]), table_name,
                                 f"ingestion_date={ingestion_date}", f"run_id={run_id}")
        # archive_path = os.path.join(resolve_path(secrets["paths"]["archive"]), table_name,
        #                              f"ingestion_date={ingestion_date}", f"run_id={run_id}")

        df.write.mode("overwrite").parquet(raw_path)
        # df.write.mode("overwrite").parquet(archive_path)

        if cfg["load_type"] == "incremental" and record_count > 0:
            watermark_col = cfg["watermark_column"]
            new_watermark = df.agg({watermark_col: "max"}).collect()[0][0]
            new_watermark = str(new_watermark) if new_watermark is not None else last_watermark
        else:
            new_watermark = last_watermark  # full loads don't use watermark; no new rows -> keep as is

        meta.update_watermark(table_name, new_watermark, "SUCCESS", record_count)
        meta.log_run(table_name, "raw", "SUCCESS",
                     records_success=record_count, start_time=start_time)

        alog.success(activity, extra={"records_extracted": record_count, "raw_path": raw_path})

    except Exception as e:
        meta.mark_failed(table_name)
        meta.log_run(table_name, "raw", "FAILED", error_message=str(e), start_time=start_time)
        alog.failed(activity, error=e)
        raise   


def run_all(table_filter=None):
    secrets = load_secrets()
    table_config = load_table_config()
    tables = get_active_tables(table_config)
    if table_filter:
        tables = [t for t in tables if t in table_filter]

    logger = get_logger(log_dir=resolve_path(secrets["paths"]["logs"]))
    alog = ActivityLogger(logger, source="source_to_raw")

    meta = MetadataManager(
        watermark_file=resolve_path(secrets["metadata"]["watermark_file"]),
        schema_registry_file=resolve_path(secrets["metadata"]["schema_registry_file"]),
        run_log_file=resolve_path(secrets["metadata"]["run_log_file"]),
    )

    spark = build_spark_session(secrets)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    failures = []
    for table_name in tables:
        cfg = table_config[table_name]
        try:
            extract_table(spark, secrets, meta, alog, table_name, cfg, run_id)
        except Exception as e:
            # one table failing should not stop the others - isolate failures per table
            failures.append((table_name, str(e)))
            continue

    spark.stop()

    if failures:
        alog.warning("run_all", f"{len(failures)} table(s) failed this run", extra={"failures": failures})
    else:
        alog.success("run_all", extra={"tables_processed": tables})

    return failures


if __name__ == "__main__":
    run_all()
