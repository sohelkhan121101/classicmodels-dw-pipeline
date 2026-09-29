"""
test_quality_rules.py
------------------------
Unit tests for src/utils/quality_rules.py - the config-driven Raw->Curated
data-quality engine (not_null, min_value, max_value, allowed_values rules,
plus the always-on duplicate-primary-key check).

"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.utils.quality_rules import apply_quality_rules, load_quality_rules, _rule_condition


# ---------------- load_quality_rules ----------------

def test_load_quality_rules_returns_empty_dict_when_file_missing():
    assert load_quality_rules("/no/such/file.json") == {}


def test_load_quality_rules_reads_real_config_file():
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "metadata", "quality_rules.json"))
    rules = load_quality_rules(path)
    assert "products" in rules
    assert "customers" in rules


# ---------------- not_null ----------------

def test_not_null_rule_catches_missing_required_column(spark):
    df = spark.createDataFrame(
        [("P1", "Widget"), ("P2", None)],
        ["productCode", "productName"],
    )
    rules = [{"type": "not_null", "columns": ["productCode", "productName"]}]
    valid_df, invalid_df = apply_quality_rules(df, rules, ["productCode"])

    assert valid_df.count() == 1
    assert invalid_df.count() == 1
    reason = invalid_df.collect()[0]["quarantine_reason"]
    assert "not_null" in reason


def test_not_null_rule_passes_when_all_required_columns_present(spark):
    df = spark.createDataFrame(
        [("P1", "Widget"), ("P2", "Gadget")],
        ["productCode", "productName"],
    )
    rules = [{"type": "not_null", "columns": ["productCode", "productName"]}]
    valid_df, invalid_df = apply_quality_rules(df, rules, ["productCode"])

    assert valid_df.count() == 2
    assert invalid_df.count() == 0


# ---------------- min_value / max_value ----------------

def test_min_value_rule_catches_value_below_threshold(spark):
    df = spark.createDataFrame(
        [("P1", 10.0), ("P2", -5.0)],
        ["productCode", "msrp"],
    )
    rules = [{"type": "min_value", "column": "msrp", "value": 0}]
    valid_df, invalid_df = apply_quality_rules(df, rules, ["productCode"])

    assert valid_df.count() == 1
    assert invalid_df.count() == 1
    assert invalid_df.collect()[0]["productCode"] == "P2"


def test_min_value_rule_ignores_nulls(spark):
    """min_value should not flag nulls - that's not_null's job, and mixing
    the two would double-quarantine the same row with a misleading reason."""
    schema = StructType([
        StructField("productCode", StringType(), True),
        StructField("msrp", DoubleType(), True),
    ])
    df = spark.createDataFrame([("P1", None)], schema)
    rules = [{"type": "min_value", "column": "msrp", "value": 0}]
    valid_df, invalid_df = apply_quality_rules(df, rules, ["productCode"])

    assert valid_df.count() == 1
    assert invalid_df.count() == 0


def test_max_value_rule_catches_value_above_threshold(spark):
    df = spark.createDataFrame(
        [("O1", 5), ("O2", 500)],
        ["orderNumber", "quantityOrdered"],
    )
    rules = [{"type": "max_value", "column": "quantityOrdered", "value": 100}]
    valid_df, invalid_df = apply_quality_rules(df, rules, ["orderNumber"])

    assert valid_df.count() == 1
    assert invalid_df.count() == 1
    assert invalid_df.collect()[0]["orderNumber"] == "O2"


# ---------------- allowed_values ----------------

def test_allowed_values_rule_catches_unexpected_value(spark):
    df = spark.createDataFrame(
        [("O1", "Shipped"), ("O2", "Teleported")],
        ["orderNumber", "status"],
    )
    rules = [{"type": "allowed_values", "column": "status", "values": ["Shipped", "Cancelled"]}]
    valid_df, invalid_df = apply_quality_rules(df, rules, ["orderNumber"])

    assert valid_df.count() == 1
    assert invalid_df.count() == 1
    assert invalid_df.collect()[0]["orderNumber"] == "O2"


# ---------------- duplicate primary key (always-on, config-independent) ----------------

def test_duplicate_primary_key_is_quarantined_not_silently_dropped(spark):
    df = spark.createDataFrame(
        [("P1", "Widget"), ("P1", "Widget Duplicate")],
        ["productCode", "productName"],
    )
    valid_df, invalid_df = apply_quality_rules(df, [], ["productCode"])

    assert valid_df.count() == 1  # first occurrence kept
    assert invalid_df.count() == 1  # second occurrence quarantined, not dropped
    assert invalid_df.collect()[0]["quarantine_reason"] == "duplicate_primary_key"


def test_composite_primary_key_duplicate_detection(spark):
    df = spark.createDataFrame(
        [(1, "P1", 5), (1, "P1", 7), (1, "P2", 3)],
        ["orderNumber", "productCode", "quantityOrdered"],
    )
    valid_df, invalid_df = apply_quality_rules(df, [], ["orderNumber", "productCode"])

    assert valid_df.count() == 2  # (1,P1) first copy + (1,P2)
    assert invalid_df.count() == 1  # (1,P1) second copy


# ---------------- multiple rules on one row: reason lists every violation ----------------

def test_row_failing_multiple_rules_lists_all_reasons():
    # exercised at unit level via _rule_condition, no need for Spark rows here
    cond1, label1 = _rule_condition({"type": "not_null", "columns": ["productName"]})
    cond2, label2 = _rule_condition({"type": "min_value", "column": "msrp", "value": 0})
    assert label1 == "not_null[productName]"
    assert label2 == "min_value[msrp<0]"


def test_row_failing_two_rules_gets_both_reasons_joined(spark):
    schema = StructType([
        StructField("productCode", StringType(), True),
        StructField("productName", StringType(), True),
        StructField("msrp", DoubleType(), True),
    ])
    df = spark.createDataFrame([("P1", None, -5.0)], schema)
    rules = [
        {"type": "not_null", "columns": ["productName"]},
        {"type": "min_value", "column": "msrp", "value": 0},
    ]
    valid_df, invalid_df = apply_quality_rules(df, rules, ["productCode"])

    assert invalid_df.count() == 1
    reason = invalid_df.collect()[0]["quarantine_reason"]
    assert "not_null[productName]" in reason
    assert "min_value[msrp<0]" in reason
    assert "; " in reason  # both reasons present, semicolon-joined


def test_unknown_rule_type_raises_at_evaluation_time():
    import pytest
    with pytest.raises(ValueError):
        _rule_condition({"type": "not_a_real_rule", "column": "x"})
