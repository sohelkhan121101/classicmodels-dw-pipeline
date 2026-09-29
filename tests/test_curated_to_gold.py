"""
test_curated_to_gold.py
--------------------------
Unit tests for src/transform/curated_to_gold.py

"""

import sys
import os
from datetime import date

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import pytest

from src.transform.curated_to_gold import (
    classify_row,
    compute_row_hash,
    scd2_merge,
    build_dim_date_rows,
)


# ---------------- classify_row ----------------

def test_classify_row_new_when_not_in_current():
    assert classify_row(in_current=False, in_curated=True, hash_matches=False) == "NEW"


def test_classify_row_changed_when_hash_differs():
    assert classify_row(in_current=True, in_curated=True, hash_matches=False) == "CHANGED"


def test_classify_row_unchanged_when_hash_matches():
    assert classify_row(in_current=True, in_curated=True, hash_matches=True) == "UNCHANGED"


def test_classify_row_missing_when_only_in_current():
    assert classify_row(in_current=True, in_curated=False, hash_matches=False) == "MISSING"


# ---------------- compute_row_hash ----------------

def test_compute_row_hash_is_deterministic():
    h1 = compute_row_hash("John", "Doe", 21000.0)
    h2 = compute_row_hash("John", "Doe", 21000.0)
    assert h1 == h2


def test_compute_row_hash_changes_when_a_value_changes():
    h1 = compute_row_hash("John", "Doe", 21000.0)
    h2 = compute_row_hash("John", "Doe", 30000.0)
    assert h1 != h2


def test_compute_row_hash_treats_none_consistently():
    # None must not crash str() concatenation, and must be stable
    h1 = compute_row_hash("John", None, 21000.0)
    h2 = compute_row_hash("John", None, 21000.0)
    assert h1 == h2


def test_compute_row_hash_none_and_empty_string_hash_identically():
    # documents current behaviour: None is normalized to "" internally,
    # so None and "" hash identically - acceptable for our source data,
    # but worth having a test that pins the behaviour down explicitly
    assert compute_row_hash(None) == compute_row_hash("")


# ---------------- scd2_merge: first run (empty current dim) ----------------

def test_scd2_merge_first_run_assigns_sequential_surrogate_keys():
    curated = pd.DataFrame({
        "employee_number": [1370, 1401],
        "first_name": ["John", "Jane"],
        "job_title": ["Sales Rep", "Sales Rep"],
        "employee_key": [0, 0],
    })
    current = pd.DataFrame()

    result, n_new, n_expired, n_missing = scd2_merge(
        curated, current, "employee_number", "employee_key", ["first_name", "job_title"], today=date(2026, 9, 27)
    )

    assert n_new == 2
    assert n_expired == 0
    assert n_missing == 0
    assert set(result["employee_key"]) == {1, 2}
    assert all(result["is_current"])
    assert all(result["row_effective_date"] == date(2026, 9, 27))
    assert all(result["row_end_date"] == date(9999, 12, 31))


# ---------------- scd2_merge: unchanged row ----------------

def test_scd2_merge_unchanged_row_keeps_same_surrogate_key():
    current = pd.DataFrame({
        "employee_key": [1],
        "employee_number": [1370],
        "first_name": ["John"],
        "job_title": ["Sales Rep"],
        "row_hash": [compute_row_hash("John", "Sales Rep")],
        "row_effective_date": [date(2020, 1, 1)],
        "row_end_date": [date(9999, 12, 31)],
        "is_current": [True],
    })
    curated = pd.DataFrame({
        "employee_number": [1370],
        "first_name": ["John"],
        "job_title": ["Sales Rep"],
        "employee_key": [0],
    })

    result, n_new, n_expired, n_missing = scd2_merge(
        curated, current, "employee_number", "employee_key", ["first_name", "job_title"], today=date(2026, 9, 27)
    )

    assert n_new == 0
    assert n_expired == 0
    assert len(result) == 1
    assert result.iloc[0]["employee_key"] == 1  # surrogate key untouched
    assert result.iloc[0]["is_current"] == True
    assert result.iloc[0]["row_effective_date"] == date(2020, 1, 1)  # not re-dated


# ---------------- scd2_merge: changed row -> expire + new version ----------------

def test_scd2_merge_changed_row_expires_old_and_creates_new_version():
    current = pd.DataFrame({
        "customer_key": [1],
        "customer_number": [103],
        "credit_limit": [21000.0],
        "row_hash": [compute_row_hash(21000.0)],
        "row_effective_date": [date(2020, 1, 1)],
        "row_end_date": [date(9999, 12, 31)],
        "is_current": [True],
    })
    curated = pd.DataFrame({
        "customer_number": [103],
        "credit_limit": [30000.0],  # changed
        "customer_key": [0],
    })

    result, n_new, n_expired, n_missing = scd2_merge(
        curated, current, "customer_number", "customer_key", ["credit_limit"], today=date(2026, 9, 27)
    )

    assert n_new == 1
    assert n_expired == 1
    assert len(result) == 2  # old expired version + new current version

    old_version = result[result["is_current"] == False].iloc[0]
    new_version = result[result["is_current"] == True].iloc[0]

    assert old_version["customer_key"] == 1
    assert old_version["credit_limit"] == 21000.0
    assert old_version["row_end_date"] == date(2026, 9, 26)  # today - 1 day

    assert new_version["customer_key"] == 2  # new surrogate key, not reused
    assert new_version["credit_limit"] == 30000.0
    assert new_version["row_effective_date"] == date(2026, 9, 27)
    assert new_version["row_end_date"] == date(9999, 12, 31)


def test_scd2_merge_next_surrogate_key_accounts_for_historical_rows_too():
    """
    Surrogate key generation must look at the max key across the WHOLE
    dim table (including already-expired historical rows), not just the
    currently-active rows - otherwise a second change on the same natural
    key could reuse a surrogate key that's already taken.
    """
    current = pd.DataFrame({
        "customer_key": [1, 2],  # 1 = old expired version, 2 = current
        "customer_number": [103, 103],
        "credit_limit": [21000.0, 30000.0],
        "row_hash": [compute_row_hash(21000.0), compute_row_hash(30000.0)],
        "row_effective_date": [date(2020, 1, 1), date(2026, 9, 27)],
        "row_end_date": [date(2026, 9, 26), date(9999, 12, 31)],
        "is_current": [False, True],
    })
    curated = pd.DataFrame({
        "customer_number": [103],
        "credit_limit": [40000.0],  # changed again
        "customer_key": [0],
    })

    result, n_new, n_expired, n_missing = scd2_merge(
        curated, current, "customer_number", "customer_key", ["credit_limit"], today=date(2026, 10, 1)
    )

    assert len(result) == 3  # two old expired versions + one new current
    new_version = result[result["is_current"] == True].iloc[0]
    assert new_version["customer_key"] == 3  # never reuses key 1 or 2


# ---------------- scd2_merge: missing row (natural key vanished from source) ----------------

def test_scd2_merge_missing_row_is_left_untouched_not_deleted():
    current = pd.DataFrame({
        "employee_key": [1],
        "employee_number": [9999],
        "first_name": ["Ghost"],
        "job_title": ["Sales Rep"],
        "row_hash": [compute_row_hash("Ghost", "Sales Rep")],
        "row_effective_date": [date(2020, 1, 1)],
        "row_end_date": [date(9999, 12, 31)],
        "is_current": [True],
    })
    curated = pd.DataFrame({
        "employee_number": [], "first_name": [], "job_title": [], "employee_key": [],
    })

    result, n_new, n_expired, n_missing = scd2_merge(
        curated, current, "employee_number", "employee_key", ["first_name", "job_title"], today=date(2026, 9, 27)
    )

    assert n_missing == 1
    assert len(result) == 1
    assert result.iloc[0]["is_current"] == True  # untouched, not force-expired


# ---------------- scd2_merge: mixed batch (new + changed + unchanged together) ----------------

def test_scd2_merge_handles_mixed_batch_correctly():
    current = pd.DataFrame({
        "product_key": [1, 2],
        "product_code": ["A", "B"],
        "msrp": [100.0, 200.0],
        "row_hash": [compute_row_hash(100.0), compute_row_hash(200.0)],
        "row_effective_date": [date(2020, 1, 1), date(2020, 1, 1)],
        "row_end_date": [date(9999, 12, 31), date(9999, 12, 31)],
        "is_current": [True, True],
    })
    curated = pd.DataFrame({
        "product_code": ["A", "B", "C"],   # A unchanged, B changed, C new
        "msrp": [100.0, 250.0, 300.0],
        "product_key": [0, 0, 0],
    })

    result, n_new, n_expired, n_missing = scd2_merge(
        curated, current, "product_code", "product_key", ["msrp"], today=date(2026, 9, 27)
    )

    assert n_new == 2   # B's new version + C
    assert n_expired == 1  # B's old version
    assert n_missing == 0
    assert len(result) == 4  # A(unchanged) + B_old(expired) + B_new + C_new

    current_rows = result[result["is_current"] == True]
    assert set(current_rows["product_code"]) == {"A", "B", "C"}
    assert set(current_rows["msrp"]) == {100.0, 250.0, 300.0}


# ---------------- build_dim_date_rows ----------------

def test_build_dim_date_rows_covers_full_inclusive_range():
    rows = build_dim_date_rows(date(2026, 1, 1), date(2026, 1, 3))
    assert len(rows) == 3
    assert rows[0]["date_key"] == 20260101
    assert rows[-1]["date_key"] == 20260103


def test_build_dim_date_rows_single_day():
    rows = build_dim_date_rows(date(2026, 1, 1), date(2026, 1, 1))
    assert len(rows) == 1
    assert rows[0]["day_name"] == "Thursday"
    assert rows[0]["quarter"] == 1
    assert rows[0]["is_weekend"] is False


def test_build_dim_date_rows_flags_weekend_correctly():
    # 2026-01-03 is a Saturday
    rows = build_dim_date_rows(date(2026, 1, 3), date(2026, 1, 4))
    assert rows[0]["is_weekend"] is True   # Saturday
    assert rows[1]["is_weekend"] is True   # Sunday
