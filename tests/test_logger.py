"""
test_logger.py
----------------
Unit tests for src/utils/logger.py

We verify the actual on-disk JSONL output, since the whole point of
"custom logs mandatory / JSON format" is that downstream tools can
parse every line as valid JSON with a fixed set of keys.
"""

import json
import logging
import pytest

from src.utils.logger import get_logger, ActivityLogger


@pytest.fixture(autouse=True)
def reset_logger_singleton():
    """
    get_logger() caches the logger by name and skips re-adding handlers.
    Between tests we must remove handlers, otherwise a later test's log
    file assertions read an EARLIER test's (now-closed) file handle.
    """
    yield
    logger = logging.getLogger("classicmodels_pipeline")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


def read_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def test_get_logger_creates_log_directory_and_file(tmp_path):
    log_dir = tmp_path / "logs"
    logger = get_logger(log_dir=str(log_dir), log_file="test.jsonl")
    logger.info("hello")

    assert (log_dir / "test.jsonl").exists()


def test_activity_logger_start_writes_status_start(tmp_path):
    logger = get_logger(log_dir=str(tmp_path), log_file="pipeline.jsonl")
    alog = ActivityLogger(logger, source="orders")

    alog.start("extract_incremental", extra={"load_type": "incremental"})

    records = read_jsonl(tmp_path / "pipeline.jsonl")
    assert records[0]["status"] == "START"
    assert records[0]["source"] == "orders"
    assert records[0]["activity"] == "extract_incremental"
    assert records[0]["duration_seconds"] is None


def test_activity_logger_success_has_required_fields(tmp_path):
    """
    Verifies the exact contract we designed: source, timestamp, who,
    duration (time_completion), status must ALL be present.
    """
    logger = get_logger(log_dir=str(tmp_path), log_file="pipeline.jsonl")
    alog = ActivityLogger(logger, source="orders")

    alog.start("extract_incremental")
    alog.success("extract_incremental", extra={"records_extracted": 42})

    records = read_jsonl(tmp_path / "pipeline.jsonl")
    success_record = records[-1]

    for required_field in ("source", "timestamp", "who", "activity", "status", "duration_seconds"):
        assert required_field in success_record, f"missing field: {required_field}"

    assert success_record["status"] == "SUCCESS"
    assert success_record["extra"]["records_extracted"] == 42
    assert success_record["duration_seconds"] is not None
    assert success_record["duration_seconds"] >= 0


def test_activity_logger_failed_includes_error_in_extra(tmp_path):
    logger = get_logger(log_dir=str(tmp_path), log_file="pipeline.jsonl")
    alog = ActivityLogger(logger, source="orders")

    alog.start("extract_incremental")
    alog.failed("extract_incremental", error="MySQL connection refused")

    records = read_jsonl(tmp_path / "pipeline.jsonl")
    failed_record = records[-1]

    assert failed_record["status"] == "FAILED"
    assert "MySQL connection refused" in failed_record["extra"]["error"]


def test_activity_logger_warning_status(tmp_path):
    logger = get_logger(log_dir=str(tmp_path), log_file="pipeline.jsonl")
    alog = ActivityLogger(logger, source="products")

    alog.warning("schema_check", "New column detected", extra={"new_columns": ["discount"]})

    records = read_jsonl(tmp_path / "pipeline.jsonl")
    assert records[-1]["status"] == "WARNING"
    assert records[-1]["extra"]["new_columns"] == ["discount"]


def test_duration_increases_between_start_and_success(tmp_path):
    import time
    logger = get_logger(log_dir=str(tmp_path), log_file="pipeline.jsonl")
    alog = ActivityLogger(logger, source="orders")

    alog.start("slow_activity")
    time.sleep(0.05)
    alog.success("slow_activity")

    records = read_jsonl(tmp_path / "pipeline.jsonl")
    assert records[-1]["duration_seconds"] >= 0.05


def test_who_field_is_not_empty(tmp_path):
    logger = get_logger(log_dir=str(tmp_path), log_file="pipeline.jsonl")
    alog = ActivityLogger(logger, source="orders")

    alog.start("extract")

    records = read_jsonl(tmp_path / "pipeline.jsonl")
    assert records[0]["who"]        # non-empty string
    assert "@" in records[0]["who"]  # format: user@hostname
