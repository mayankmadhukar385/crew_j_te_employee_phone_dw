"""Tests for away_priority_pipeline.py (mirrors home pipeline with CALL_LIST='AWAY')."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.transformations.away_priority_pipeline import build_away_schedule
from src.transformations.phone_router import route_by_phone_type
from tests.fixtures.sample_data import make_source_row


def _away_df(spark: SparkSession, row_override: dict):
    row = make_source_row(**row_override)

    schema = StructType([
        StructField(col, StringType(), True) for col in row.keys()
    ])

    df = spark.createDataFrame([row], schema=schema)
    _, _, _, _, away_df, _ = route_by_phone_type(df, {})
    return away_df


def test_pivot_3_away_groups_produce_15_rows(spark):
    """1 employee with all 3 AWAY SEQ groups → 15 rows after build_away_schedule."""
    overrides = {}
    for g in range(1, 4):
        overrides[f"TELE_AWAY_PRI_FROM_{g}"] = "0900"
        overrides[f"TELE_AWAY_PRI_TO_{g}"] = "1800"
        for s in range(1, 6):
            overrides[f"TELE_AWAY_PRI_SEQ_{s}_{g}"] = str(s)
    away_df = _away_df(spark, overrides)
    result = build_away_schedule(away_df)
    assert result.count() == 15


def test_call_list_assigned_away(spark):
    """All rows from build_away_schedule have CALL_LIST='AWAY'."""
    overrides = {
        "TELE_AWAY_PRI_FROM_1": "0900",
        "TELE_AWAY_PRI_TO_1": "1800",
        "TELE_AWAY_PRI_SEQ_1_1": "1",
    }
    away_df = _away_df(spark, overrides)
    result = build_away_schedule(away_df)
    distinct = {r["CALL_LIST"] for r in result.collect()}
    assert distinct == {"AWAY"}


def test_away_invalid_time_nulled(spark):
    """TELE_AWAY_PRI_FROM='9900' (invalid) → null after time validation."""
    overrides = {
        "TELE_AWAY_PRI_FROM_1": "9900",
        "TELE_AWAY_PRI_TO_1": "1800",
        "TELE_AWAY_PRI_SEQ_1_1": "1",
    }
    away_df = _away_df(spark, overrides)
    result = build_away_schedule(away_df)
    r = result.collect()[0]
    assert r["TELE_HOME_PRI_FROM"] is None
