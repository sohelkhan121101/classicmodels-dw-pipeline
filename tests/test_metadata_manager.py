"""
test_metadata_manager.py
--------------------------
Unit tests for src/utils/metadata_manager.py

This is the MOST important module to test thoroughly, because it holds
the self-healing / incremental-load guarantee:

    "On failure, the watermark must NOT move forward."

Every test here uses tmp_path so we never touch the real
metadata/watermark_state.json used by the actual pipeline.
"""

import json
import os
import pytest

from src.utils.metadata_manager import MetadataManager


@pytest.fixture
def meta(tmp_path):
    return MetadataManager(
        watermark_file=str(tmp_path / "watermark_state.json"),
        schema_registry_file=str(tmp_path / "schema_registry.json"),
        run_log_file=str(tmp_path / "logs" / "pipeline_run_log.jsonl"),
    )


# ---------------- watermark tests ----------------

def test_get_watermark_returns_none_when_table_never_seen(meta):
    assert meta.get_watermark("orders") is None


def test_update_watermark_persists_value_and_can_be_read_back(meta):
    meta.update_watermark("orders", "2026-09-14", "SUCCESS", records_processed=50)

    assert meta.get_watermark("orders") == "2026-09-14"


def test_update_watermark_overwrites_previous_value(meta):
    meta.update_watermark("orders", "2026-09-10", "SUCCESS", records_processed=10)
    meta.update_watermark("orders", "2026-09-14", "SUCCESS", records_processed=5)

    assert meta.get_watermark("orders") == "2026-09-14"


def test_mark_failed_does_not_move_the_watermark(meta):
    """
    THE core self-healing guarantee: a failed run must leave the watermark
    exactly where it was, so the next scheduled run retries the same window
    automatically - no human needs to reset anything.
    """
    meta.update_watermark("orders", "2026-09-10", "SUCCESS", records_processed=10)

    meta.mark_failed("orders")

    assert meta.get_watermark("orders") == "2026-09-10"


def test_mark_failed_sets_status_to_failed(meta):
    meta.update_watermark("payments", "2026-09-10", "SUCCESS", records_processed=3)
    meta.mark_failed("payments")

    state = meta._read_json(meta.watermark_file)
    assert state["payments"]["last_run_status"] == "FAILED"


def test_watermarks_for_different_tables_are_independent(meta):
    meta.update_watermark("orders", "2026-09-14", "SUCCESS", records_processed=5)
    meta.update_watermark("payments", "2026-09-01", "SUCCESS", records_processed=2)

    assert meta.get_watermark("orders") == "2026-09-14"
    assert meta.get_watermark("payments") == "2026-09-01"


# ---------------- schema registry tests ----------------

def test_get_known_schema_returns_none_for_new_table(meta):
    assert meta.get_known_schema("products") is None


def test_update_schema_stores_and_retrieves_schema_with_version(meta):
    schema = {"productCode": "StringType", "buyPrice": "DecimalType"}
    meta.update_schema("products", schema, version=1)

    result = meta.get_known_schema("products")

    assert result["schema"] == schema
    assert result["version"] == 1


def test_update_schema_bumps_version_on_second_call(meta):
    meta.update_schema("products", {"productCode": "StringType"}, version=1)
    meta.update_schema("products", {"productCode": "StringType", "newCol": "IntegerType"}, version=2)

    result = meta.get_known_schema("products")
    assert result["version"] == 2
    assert "newCol" in result["schema"]


# ---------------- run log tests ----------------

def test_log_run_appends_jsonl_line(meta):
    meta.log_run("orders", "raw+archive", "SUCCESS", records_success=100)

    with open(meta.run_log_file) as f:
        lines = f.readlines()

    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["table_name"] == "orders"
    assert record["status"] == "SUCCESS"
    assert record["records_success"] == 100


def test_log_run_appends_multiple_runs_without_overwriting(meta):
    meta.log_run("orders", "raw+archive", "SUCCESS", records_success=10)
    meta.log_run("orders", "curated", "SUCCESS", records_success=9, records_quarantined=1)

    with open(meta.run_log_file) as f:
        lines = f.readlines()

    assert len(lines) == 2
    assert json.loads(lines[0])["layer"] == "raw+archive"
    assert json.loads(lines[1])["layer"] == "curated"


def test_log_run_creates_parent_directory_if_missing(tmp_path):
    nested_log_path = tmp_path / "deep" / "nested" / "logs" / "run.jsonl"
    m = MetadataManager(
        watermark_file=str(tmp_path / "wm.json"),
        schema_registry_file=str(tmp_path / "sr.json"),
        run_log_file=str(nested_log_path),
    )

    m.log_run("customers", "raw+archive", "SUCCESS")

    assert nested_log_path.exists()
