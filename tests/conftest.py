"""
conftest.py
-------------
Shared pytest fixtures. A single session-scoped SparkSession, reused by
every test file that needs one (spinning up a new one per test is slow
and unnecessary - Spark itself is stateless enough across our tests).
"""
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from pyspark.sql import SparkSession

from src.utils.spark_utils import configure_pyspark_python


@pytest.fixture(scope="session")
def spark():
    configure_pyspark_python()
    spark = (
        SparkSession.builder
        .appName("classicmodels_pipeline_tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
