"""
test_mysql_manager.py
-------------------------
Unit tests for src/warehouse/mysql_manager.py
"""

import os
import sys
from datetime import date

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from src.utils.config_loader import load_secrets
from src.warehouse.mysql_manager import MySQLManager

TEST_DB = "classicmodels_dw_test"


def _server_reachable(mysql_cfg):
    try:
        engine = create_engine(
            f"mysql+pymysql://{mysql_cfg['user']}:{mysql_cfg['password']}"
            f"@{mysql_cfg['host']}:{mysql_cfg['port']}"
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except OperationalError:
        return False


def _base_mysql_cfg():
    secrets = load_secrets()
    cfg = dict(secrets["warehouse"]["mysql"])
    cfg["database"] = TEST_DB
    return cfg


try:
    _cfg = _base_mysql_cfg()
    _skip_reason = None if _server_reachable(_cfg) else "No MySQL server reachable at configured host/port"
except Exception as e:
    _cfg = None
    _skip_reason = f"Could not load secrets.yaml / mysql config: {e}"

pytestmark = pytest.mark.skipif(_skip_reason is not None, reason=_skip_reason or "")


@pytest.fixture
def dw():
    manager = MySQLManager(_cfg)
    manager.ensure_schema()
    yield manager
    # clean slate between tests
    with manager.engine.begin() as conn:
        for table in ["dim_date", "dim_office", "dim_product_line", "dim_order_status",
                      "dim_employee", "dim_product", "dim_customer",
                      "fact_order_line", "fact_payment"]:
            conn.execute(text(f"TRUNCATE TABLE `{table}`"))


def test_ensure_schema_creates_all_tables(dw):
    for table in ["dim_date", "dim_office", "dim_product_line", "dim_order_status",
                  "dim_employee", "dim_product", "dim_customer",
                  "fact_order_line", "fact_payment"]:
        assert dw.table_exists(table), f"{table} was not created"


def test_read_table_returns_empty_dataframe_when_table_missing(dw):
    result = dw.read_table("no_such_table")
    assert result.empty


def test_overwrite_table_writes_and_reads_back_data(dw):
    df = pd.DataFrame({
        "office_key": [1], "office_code": ["1"], "city": ["SF"], "phone": ["555"],
        "address_line1": ["100 Market"], "address_line2": [None], "state": ["CA"],
        "country": ["USA"], "postal_code": ["94105"], "territory": ["NA"],
    })
    dw.overwrite_table("dim_office", df)
    result = dw.read_table("dim_office")

    assert len(result) == 1
    assert result.iloc[0]["city"] == "SF"


def test_overwrite_table_replaces_previous_contents_entirely(dw):
    df1 = pd.DataFrame({
        "office_key": [1], "office_code": ["1"], "city": ["SF"], "phone": ["555"],
        "address_line1": ["A"], "address_line2": [None], "state": ["CA"],
        "country": ["USA"], "postal_code": ["1"], "territory": ["NA"],
    })
    df2 = pd.DataFrame({
        "office_key": [2], "office_code": ["2"], "city": ["NYC"], "phone": ["666"],
        "address_line1": ["B"], "address_line2": [None], "state": ["NY"],
        "country": ["USA"], "postal_code": ["2"], "territory": ["NA"],
    })
    dw.overwrite_table("dim_office", df1)
    dw.overwrite_table("dim_office", df2)
    result = dw.read_table("dim_office")

    assert len(result) == 1
    assert result.iloc[0]["city"] == "NYC"


def test_overwrite_table_column_order_mismatch_still_lands_correctly(dw):
    """pandas.to_sql() inserts by explicit column name, so a DataFrame
    whose column order doesn't match the DDL still lands correctly -
    the MySQL equivalent of the DuckDB "INSERT ... BY NAME" fix."""
    df = pd.DataFrame({
        "is_current": [1],
        "row_end_date": [date(9999, 12, 31)],
        "customer_key": [1],
        "credit_limit": [21000.0],
        "customer_number": [103],
        "row_hash": ["abc123"],
        "row_effective_date": [date(2026, 1, 1)],
        "customer_name": ["Atelier graphique"],
        "contact_last_name": [None], "contact_first_name": [None], "phone": [None],
        "address_line1": [None], "address_line2": [None], "city": [None],
        "state": [None], "postal_code": [None], "country": [None],
        "sales_rep_employee_key": [1],
    })
    dw.overwrite_table("dim_customer", df)
    result = dw.read_table("dim_customer")

    row = result.iloc[0]
    assert row["credit_limit"] == 21000.0
    assert row["customer_name"] == "Atelier graphique"


def test_append_table_adds_without_removing_existing_rows(dw):
    df1 = pd.DataFrame({
        "check_number": ["A1"], "customer_key": [1], "payment_date_key": [20260101], "payment_amount": [100.0],
    })
    df2 = pd.DataFrame({
        "check_number": ["A2"], "customer_key": [1], "payment_date_key": [20260102], "payment_amount": [200.0],
    })
    dw.overwrite_table("fact_payment", df1)
    dw.append_table("fact_payment", df2)
    result = dw.read_table("fact_payment")

    assert len(result) == 2
    assert set(result["check_number"]) == {"A1", "A2"}


def test_append_table_no_op_on_empty_dataframe(dw):
    empty_df = pd.DataFrame(columns=["check_number", "customer_key", "payment_date_key", "payment_amount"])
    dw.append_table("fact_payment", empty_df)  # should not raise
    result = dw.read_table("fact_payment")
    assert result.empty


def test_get_max_surrogate_key_returns_zero_when_table_empty(dw):
    assert dw.get_max_surrogate_key("dim_customer", "customer_key") == 0


def test_get_max_surrogate_key_returns_current_max(dw):
    df = pd.DataFrame({
        "office_key": [1, 2, 3], "office_code": ["1", "2", "3"], "city": ["A", "B", "C"],
        "phone": [None, None, None], "address_line1": [None, None, None],
        "address_line2": [None, None, None], "state": [None, None, None],
        "country": [None, None, None], "postal_code": [None, None, None], "territory": [None, None, None],
    })
    dw.overwrite_table("dim_office", df)
    assert dw.get_max_surrogate_key("dim_office", "office_key") == 3
