"""Tests for home_phone_join.py (HOME_BASIC_JNR → NEW_PRTY_TFM → MAX_PRTY_AGG → BASIC_MAX_JNR → ADJUSTING_PRTY_TFM)."""

import pytest
from chispa import assert_df_equality
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from src.transformations.home_phone_join import (
    assign_home_adjusted_priority,
    assign_home_scheduled_priority,
    compute_home_max_priority,
    join_home_max_to_basic,
    join_home_schedule_to_basic,
)


def _sched_schema():
    return StructType([
        StructField("EMP_NBR", StringType()),
        StructField("CALL_LIST", StringType()),
        StructField("CALL_PRTY", IntegerType()),
        StructField("LKP_CALL_PRTY", IntegerType()),
        StructField("TELE_HOME_PRI_FROM", StringType()),
        StructField("TELE_HOME_PRI_TO", StringType()),
        StructField("UPD_TS", StringType()),
        StructField("USER_ID", StringType()),
    ])


def _basic_schema():
    return StructType([
        StructField("EMP_NBR", StringType()),
        StructField("LKP_CALL_PRTY", IntegerType()),
        StructField("PH_NBR", StringType()),
        StructField("PH_ACCESS", StringType()),
        StructField("PH_COMMENTS", StringType()),
        StructField("PH_TYPE", StringType()),
        StructField("UNLISTD_IND", StringType()),
        StructField("HOME_AWAY_IND", StringType()),
        StructField("TEMP_PH_EXP_TS", StringType()),
        StructField("CALL_PRTY", IntegerType()),
        StructField("UPD_TS", StringType()),
        StructField("USER_ID", StringType()),
    ])


def test_inner_join_matches_by_emp_and_priority(spark):
    """Matching EMP_NBR + LKP_CALL_PRTY → joined row produced."""
    sched_df = spark.createDataFrame(
        [("001", "HOME", 1, 2, "0800", "1700", None, "USR")],
        _sched_schema(),
    )
    basic_df = spark.createDataFrame(
        [("001", 2, "2125551234", None, None, "W", "N", "H", None, 2, None, "USR")],
        _basic_schema(),
    )
    result = join_home_schedule_to_basic(sched_df, basic_df)
    assert result.count() == 1
    r = result.collect()[0]
    assert r["EMP_NBR"] == "001"
    assert r["PH_NBR"] == "2125551234"


def test_inner_join_excludes_unmatched(spark):
    """No matching LKP_CALL_PRTY → no output row."""
    sched_df = spark.createDataFrame(
        [("001", "HOME", 1, 3, "0800", "1700", None, "USR")],
        _sched_schema(),
    )
    basic_df = spark.createDataFrame(
        [("001", 9, "2125551234", None, None, "W", "N", "H", None, 9, None, "USR")],
        _basic_schema(),
    )
    result = join_home_schedule_to_basic(sched_df, basic_df)
    assert result.count() == 0


def test_row_counter_resets_per_employee(spark):
    """3 rows for EMP_NBR=001, 2 rows for EMP_NBR=002 → CALL_PRTY resets per employee."""
    joined_schema = StructType([
        StructField("EMP_NBR", StringType()),
        StructField("CALL_LIST", StringType()),
        StructField("CALL_PRTY", IntegerType()),
        StructField("TELE_HOME_PRI_FROM", StringType()),
        StructField("TELE_HOME_PRI_TO", StringType()),
        StructField("PH_NBR", StringType()),
        StructField("PH_ACCESS", StringType()),
        StructField("PH_COMMENTS", StringType()),
        StructField("PH_TYPE", StringType()),
        StructField("UNLISTD_IND", StringType()),
        StructField("HOME_AWAY_IND", StringType()),
        StructField("TEMP_PH_EXP_TS", StringType()),
        StructField("UPD_TS", StringType()),
        StructField("USER_ID", StringType()),
    ])
    rows = [
        ("001", "HOME", 1, "0800", "1700", "2125551001", None, None, None, None, None, None, None, "USR"),
        ("001", "HOME", 2, "0900", "1800", "2125551002", None, None, None, None, None, None, None, "USR"),
        ("001", "HOME", 3, "1000", "1900", "2125551003", None, None, None, None, None, None, None, "USR"),
        ("002", "HOME", 1, "0800", "1700", "2125552001", None, None, None, None, None, None, None, "USR"),
        ("002", "HOME", 2, "0900", "1800", "2125552002", None, None, None, None, None, None, None, "USR"),
    ]
    df = spark.createDataFrame(rows, joined_schema)
    to_funnel_df, _ = assign_home_scheduled_priority(df)
    result = to_funnel_df.collect()
    emp001_prty = sorted(r["CALL_PRTY"] for r in result if r["EMP_NBR"] == "001")
    emp002_prty = sorted(r["CALL_PRTY"] for r in result if r["EMP_NBR"] == "002")
    assert emp001_prty == [1, 2, 3]
    assert emp002_prty == [1, 2]


def test_max_priority_aggregation_chispa(spark):
    """compute_home_max_priority returns correct MAX per EMP_NBR — verified with chispa."""
    schema = StructType([
        StructField("EMP_NBR", StringType()),
        StructField("CALL_PRTY", IntegerType()),
    ])
    rows = [("001", 1), ("001", 3), ("001", 2), ("002", 5)]
    df = spark.createDataFrame(rows, schema)
    from src.transformations.home_phone_join import compute_home_max_priority
    result = compute_home_max_priority(df)
    expected = spark.createDataFrame(
        [("001", 3), ("002", 5)],
        StructType([StructField("EMP_NBR", StringType()), StructField("MAX_CALL_PRTY", IntegerType())]),
    )
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)


def test_adjusting_prty_starts_after_max(spark):
    """MAX_CALL_PRTY=3 for EMP_NBR='001', 2 unscheduled rows → CALL_PRTY=4,5."""
    basic_schema = StructType([
        StructField("EMP_NBR", StringType()),
        StructField("MAX_CALL_PRTY", IntegerType()),
        StructField("CALL_PRTY", IntegerType()),
        StructField("CALL_LIST", StringType()),
        StructField("PH_NBR", StringType()),
        StructField("PH_ACCESS", StringType()),
        StructField("PH_COMMENTS", StringType()),
        StructField("PH_TYPE", StringType()),
        StructField("UNLISTD_IND", StringType()),
        StructField("HOME_AWAY_IND", StringType()),
        StructField("TEMP_PH_EXP_TS", StringType()),
        StructField("UPD_TS", StringType()),
        StructField("USER_ID", StringType()),
    ])
    rows = [
        ("001", 3, 2, "BASIC", "2125551001", None, None, None, None, "H", None, None, "USR"),
        ("001", 3, 3, "BASIC", "2125551002", None, None, None, None, "H", None, None, "USR"),
    ]
    df = spark.createDataFrame(rows, basic_schema)
    result = assign_home_adjusted_priority(df)
    priorities = sorted(r["CALL_PRTY"] for r in result.collect())
    assert priorities == [4, 5]
