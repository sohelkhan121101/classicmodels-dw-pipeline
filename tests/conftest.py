"""
conftest.py
------------
Shared fixtures for the whole test suite.

A single session-scoped local[1] SparkSession is reused across all tests
that need a real DataFrame (query-building logic and quality-rule logic
are tested against actual PySpark DataFrames, not mocks, since that's
where real bugs - wrong column names, wrong filter conditions - show up).
"""

import sys
import os
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("classicmodels_pipeline_tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
