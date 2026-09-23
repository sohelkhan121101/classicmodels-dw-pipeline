"""
test_source_to_raw.py
------------------------
Unit tests for src/extract/source_to_raw.py

Focus areas (the parts with real business logic, NOT the Spark JDBC I/O
itself - we never hit a real MySQL server in unit tests):

  1. build_extract_query - full vs incremental vs incremental-with-lookback
  2. build_jdbc_url
  3. check_schema_evolution - must NEVER raise, must correctly version
     new/changed/dropped columns
"""

import pytest
from unittest.mock import MagicMock

from src.extract.source_to_raw import build_extract_query, build_jdbc_url, check_schema_evolution


# ---------------- build_jdbc_url ----------------

def test_build_jdbc_url_formats_correctly():
    mysql_cfg = {"host": "localhost", "port": 3306, "database": "classicmodels"}

    url = build_jdbc_url(mysql_cfg)

    assert url == "jdbc:mysql://localhost:3306/classicmodels"


# ---------------- build_extract_query ----------------

def test_full_load_ignores_watermark_entirely():
    cfg = {"load_type": "full"}

    query = build_extract_query("offices", cfg, last_watermark="anything")

    assert query == "(SELECT * FROM offices) AS t"
    assert "WHERE" not in query


def test_incremental_without_lookback_uses_simple_greater_than():
    # orderdetails: monotonic orderNumber, no date, no lookback
    cfg = {"load_type": "incremental", "watermark_column": "orderNumber", "lookback_days": 0}

    query = build_extract_query("orderdetails", cfg, last_watermark="10100")

    assert query == "(SELECT * FROM orderdetails WHERE orderNumber > '10100') AS t"


def test_incremental_with_lookback_subtracts_days_from_watermark():
    # orders: watermark=orderDate, lookback=7 days -> re-pull last 7 days to
    # catch late status/shippedDate updates
    cfg = {"load_type": "incremental", "watermark_column": "orderDate", "lookback_days": 7}

    query = build_extract_query("orders", cfg, last_watermark="2026-09-14")

    assert query == "(SELECT * FROM orders WHERE orderDate >= '2026-09-07') AS t"


def test_incremental_with_lookback_handles_genesis_watermark():
    # first-ever run: watermark is the "1900-01-01" seed value. The lookback
    # math still applies (parses fine as a real date), so it correctly
    # pulls everything from day 1 - this just documents that behaviour so
    # a future change to the seed value doesn't silently break extraction.
    cfg = {"load_type": "incremental", "watermark_column": "orderDate", "lookback_days": 7}

    query = build_extract_query("orders", cfg, last_watermark="1900-01-01")

    assert query == "(SELECT * FROM orders WHERE orderDate >= '1899-12-25') AS t"


def test_incremental_with_lookback_handles_none_watermark_gracefully():
    cfg = {"load_type": "incremental", "watermark_column": "paymentDate", "lookback_days": 7}

    # None should fall back to genesis rather than crashing strptime
    query = build_extract_query("payments", cfg, last_watermark=None)

    assert "SELECT * FROM payments WHERE paymentDate >=" in query


# ---------------- check_schema_evolution ----------------

def _make_fake_df(columns_with_types):
    """Builds a lightweight fake Spark DataFrame with just a .schema.fields."""
    df = MagicMock()
    fields = []
    for name, dtype in columns_with_types.items():
        field = MagicMock()
        field.name = name
        field.dataType = dtype
        fields.append(field)
    df.schema.fields = fields
    return df


def test_first_time_schema_is_registered_as_version_1():
    meta = MagicMock()
    meta.get_known_schema.return_value = None
    alog = MagicMock()

    df = _make_fake_df({"productCode": "StringType", "buyPrice": "DecimalType"})

    check_schema_evolution(meta, alog, "products", df)

    meta.update_schema.assert_called_once()
    args, kwargs = meta.update_schema.call_args
    assert args[0] == "products"
    assert kwargs.get("version") == 1
    alog.warning.assert_called_once()


def test_no_schema_change_does_not_call_update_schema():
    meta = MagicMock()
    meta.get_known_schema.return_value = {
        "schema": {"productCode": "StringType", "buyPrice": "DecimalType"},
        "version": 1,
    }
    alog = MagicMock()

    df = _make_fake_df({"productCode": "StringType", "buyPrice": "DecimalType"})

    check_schema_evolution(meta, alog, "products", df)

    meta.update_schema.assert_not_called()
    alog.warning.assert_not_called()


def test_new_column_detected_bumps_version_and_warns_but_never_raises():
    meta = MagicMock()
    meta.get_known_schema.return_value = {
        "schema": {"productCode": "StringType"},
        "version": 1,
    }
    alog = MagicMock()

    df = _make_fake_df({"productCode": "StringType", "discountPercent": "DoubleType"})

    # must not raise - this is the "schema evolution must not break pipeline" contract
    check_schema_evolution(meta, alog, "products", df)

    meta.update_schema.assert_called_once()
    args, kwargs = meta.update_schema.call_args
    assert kwargs.get("version") == 2  # bumped from v1 to v2
    alog.warning.assert_called_once()


def test_dropped_column_is_detected_and_logged():
    meta = MagicMock()
    meta.get_known_schema.return_value = {
        "schema": {"productCode": "StringType", "buyPrice": "DecimalType"},
        "version": 1,
    }
    alog = MagicMock()

    df = _make_fake_df({"productCode": "StringType"})  # buyPrice missing now

    check_schema_evolution(meta, alog, "products", df)

    meta.update_schema.assert_called_once()
    alog.warning.assert_called_once()


def test_changed_column_type_is_detected():
    meta = MagicMock()
    meta.get_known_schema.return_value = {
        "schema": {"quantityOrdered": "IntegerType"},
        "version": 1,
    }
    alog = MagicMock()

    df = _make_fake_df({"quantityOrdered": "LongType"})  # type changed

    check_schema_evolution(meta, alog, "orderdetails", df)

    meta.update_schema.assert_called_once()
    alog.warning.assert_called_once()
