"""
test_raw_to_curated.py
------------------------
Unit tests for src/transform/raw_to_curated.py

apply_quality_rules is tested against REAL Spark DataFrames (via the
shared `spark` fixture in conftest.py) because this is exactly the kind
of filter logic where a typo in a column-null-check silently passes
everything through - a mock would never catch that.
"""

import os
import pytest

from src.transform.raw_to_curated import apply_quality_rules, get_latest_raw_path


# ---------------- apply_quality_rules ----------------

def test_rows_with_all_pk_columns_present_are_valid(spark):
    df = spark.createDataFrame(
        [("S10_1678", "1969 Harley Davidson", 95),
         ("S10_1949", "1952 Alpine Renault", 68)],
        ["productCode", "productName", "quantityInStock"],
    )

    valid_df, invalid_df = apply_quality_rules(df, primary_key_cols=["productCode"])

    assert valid_df.count() == 2
    assert invalid_df.count() == 0


def test_row_with_null_single_pk_column_is_quarantined(spark):
    df = spark.createDataFrame(
        [("S10_1678", "1969 Harley Davidson"),
         (None, "Unknown Product")],
        ["productCode", "productName"],
    )

    valid_df, invalid_df = apply_quality_rules(df, primary_key_cols=["productCode"])

    assert valid_df.count() == 1
    assert invalid_df.count() == 1
    assert valid_df.collect()[0]["productCode"] == "S10_1678"


def test_composite_pk_row_invalid_if_any_key_column_is_null():
    # orderdetails has a composite PK: (orderNumber, productCode)
    pass  # implemented below with spark fixture


def test_composite_pk_requires_all_columns_non_null(spark):
    df = spark.createDataFrame(
        [(10100, "S10_1678", 30),
         (10100, None, 20),          # productCode missing -> invalid
         (None, "S10_1949", 15)],    # orderNumber missing -> invalid
        ["orderNumber", "productCode", "quantityOrdered"],
    )

    valid_df, invalid_df = apply_quality_rules(df, primary_key_cols=["orderNumber", "productCode"])

    assert valid_df.count() == 1
    assert invalid_df.count() == 2


def test_empty_dataframe_produces_empty_valid_and_invalid(spark):
    df = spark.createDataFrame([], "productCode STRING, productName STRING")

    valid_df, invalid_df = apply_quality_rules(df, primary_key_cols=["productCode"])

    assert valid_df.count() == 0
    assert invalid_df.count() == 0


def test_all_rows_valid_means_zero_quarantine(spark):
    df = spark.createDataFrame(
        [(1370, "Sales Rep"), (1371, "Sales Rep")],
        ["employeeNumber", "jobTitle"],
    )

    valid_df, invalid_df = apply_quality_rules(df, primary_key_cols=["employeeNumber"])

    assert valid_df.count() == 2
    assert invalid_df.count() == 0


def test_all_rows_invalid_means_zero_valid(spark):
    # explicit schema needed here - Spark can't infer a type when EVERY
    # value in a column is None
    df = spark.createDataFrame(
        [(None, "Sales Rep"), (None, "Manager")],
        "employeeNumber INT, jobTitle STRING",
    )

    valid_df, invalid_df = apply_quality_rules(df, primary_key_cols=["employeeNumber"])

    assert valid_df.count() == 0
    assert invalid_df.count() == 2


# ---------------- deduplication behaviour (documents current design) ----------------

def test_duplicate_primary_keys_are_deduplicated_after_quality_split(spark):
    """
    apply_quality_rules itself does NOT dedupe (that happens right after,
    in process_table via dropDuplicates). This test documents that a
    duplicate valid row survives apply_quality_rules unchanged, and
    dropDuplicates is what collapses it - so if someone ever removes the
    dropDuplicates call in process_table, THIS test still passes but the
    pipeline behaviour silently regresses. Kept here as a canary + a
    direct test of dropDuplicates on the valid_df output.
    """
    df = spark.createDataFrame(
        [("S10_1678", "1969 Harley Davidson"),
         ("S10_1678", "1969 Harley Davidson (dup)")],
        ["productCode", "productName"],
    )

    valid_df, _ = apply_quality_rules(df, primary_key_cols=["productCode"])
    assert valid_df.count() == 2  # not deduped yet at this stage

    deduped = valid_df.dropDuplicates(["productCode"])
    assert deduped.count() == 1


# ---------------- get_latest_raw_path ----------------

def test_get_latest_raw_path_picks_most_recent_date_and_run(tmp_path):
    table_dir = tmp_path / "orders"
    (table_dir / "ingestion_date=2026-09-13" / "run_id=100").mkdir(parents=True)
    (table_dir / "ingestion_date=2026-09-14" / "run_id=200").mkdir(parents=True)
    (table_dir / "ingestion_date=2026-09-14" / "run_id=300").mkdir(parents=True)

    result = get_latest_raw_path(str(tmp_path), "orders")

    assert result.endswith("ingestion_date=2026-09-14/run_id=300")


def test_get_latest_raw_path_raises_when_table_has_no_raw_data(tmp_path):
    with pytest.raises(FileNotFoundError):
        get_latest_raw_path(str(tmp_path), "nonexistent_table")


def test_get_latest_raw_path_raises_when_date_dir_has_no_run_ids(tmp_path):
    table_dir = tmp_path / "orders"
    (table_dir / "ingestion_date=2026-09-14").mkdir(parents=True)  # empty, no run_id= subfolder

    with pytest.raises(FileNotFoundError):
        get_latest_raw_path(str(tmp_path), "orders")
