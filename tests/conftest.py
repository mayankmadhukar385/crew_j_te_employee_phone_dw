"""Pytest configuration and shared fixtures."""

import os
import sys

import pytest
from pyspark.sql import SparkSession
from src.utils.config_loader import load_config


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a test SparkSession."""
    python_exec = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_crew_j_te_employee_phone_dw")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .config("spark.executorEnv.PYSPARK_PYTHON", python_exec)
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def config() -> dict:
    """Load test configuration."""
    return load_config("configs")