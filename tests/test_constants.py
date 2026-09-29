"""
test_constants.py
--------------------
Confirms every constant in src/utils/constants.py falls back to its
documented default when no environment variable is set, AND can be
overridden via env var - this is a fresh-subprocess test because the
module reads os.environ at IMPORT time.
"""

import os
import subprocess
import sys


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _run_and_print(env_overrides: dict, expr: str) -> str:
    env = os.environ.copy()
    env.update(env_overrides)
    result = subprocess.run(
        [sys.executable, "-c", f"import sys; sys.path.insert(0, '{PROJECT_ROOT}'); from src.utils.constants import {expr.split('.')[0] if '.' in expr else expr}; print({expr})"],
        env=env, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_scd2_open_end_date_default():
    out = _run_and_print({}, "SCD2_OPEN_END_DATE")
    assert out == "9999-12-31"


def test_scd2_open_end_date_override():
    out = _run_and_print({"CLASSICMODELS_SCD2_OPEN_END_DATE": "2099-01-01"}, "SCD2_OPEN_END_DATE")
    assert out == "2099-01-01"


def test_lookback_base_date_default():
    out = _run_and_print({}, "LOOKBACK_BASE_DATE")
    assert out == "1900-01-01"


def test_lookback_base_date_override():
    out = _run_and_print({"CLASSICMODELS_LOOKBACK_BASE_DATE": "2000-01-01"}, "LOOKBACK_BASE_DATE")
    assert out == "2000-01-01"


def test_activity_log_file_default():
    out = _run_and_print({}, "DEFAULT_ACTIVITY_LOG_FILE")
    assert out == "pipeline.jsonl"


def test_activity_log_file_override():
    out = _run_and_print({"CLASSICMODELS_ACTIVITY_LOG_FILE": "custom.jsonl"}, "DEFAULT_ACTIVITY_LOG_FILE")
    assert out == "custom.jsonl"
