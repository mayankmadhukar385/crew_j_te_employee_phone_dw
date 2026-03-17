"""Tests for away_phone_join.py (AWAY_BASIC_JNR → ABREC_NEW_PRTY_TFM → MAX_ABPRTY_AGG → AWAY_MAX_JNR → ADJUST_PRTY_TFM)."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.transformations.away_phone_join import (
    assign_away_adjusted_priority,
    assign_away_scheduled_priority,
    compute_away_max_priority,
    join_away_max_to_basic,
    join_away_schedule_to_basic,
)


def test_away_join_matches_by_emp_and_priority(spark):
    """Matching EMP_NBR + LKP_CALL_PRTY → joined row produced with AWAY semantics."""
    sched_schema = StructType([
        StructField("EMP_NBR", StringType()),
        StructField("CALL_LIST", StringType()),
        StructField("CALL_PRTY", IntegerType()),
        StructField("LKP_CALL_PRTY", IntegerType()),
        StructField("TELE_HOME_PRI_FROM", StringType()),
        StructField("TELE_HOME_PRI_TO", StringType()),
        StructField("UPD_TS", StringType()),
        StructField("USER_ID", StringType()),
    ])
    basic_schema = StructType([
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
    sched_df = spark.createDataFrame([("001", "AWAY", 1, 2, "0900", "1800", None, "USR")], sched_schema)
    basic_df = spark.createDataFrame([("001", 2, "2125551234", None, None, None, None, "A", None, 2, None, "USR")], basic_schema)
    result = join_away_schedule_to_basic(sched_df, basic_df)
    assert result.count() == 1


def test_away_adjusted_priority_call_list_is_away(spark):
    """assign_away_adjusted_priority sets CALL_LIST='AWAY'."""
    schema = StructType([
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
    df = spark.createDataFrame([("001", 2, 2, "BASIC", "2125551001", None, None, None, None, "A", None, None, "USR")], schema)
    result = assign_away_adjusted_priority(df)
    r = result.collect()[0]
    assert r["CALL_LIST"] == "AWAY"
    assert r["EFF_START_TM"] == "0001"
    assert r["EFF_END_TM"] == "2359"


def test_away_max_priority_aggregation(spark):
    """compute_away_max_priority returns MAX(CALL_PRTY) per EMP_NBR."""
    schema = StructType([
        StructField("EMP_NBR", StringType()),
        StructField("CALL_PRTY", IntegerType()),
    ])
    rows = [("001", 1), ("001", 3), ("001", 2), ("002", 4)]
    df = spark.createDataFrame(rows, schema)
    result = compute_away_max_priority(df)
    by_emp = {r["EMP_NBR"]: r["MAX_CALL_PRTY"] for r in result.collect()}
    assert by_emp["001"] == 3
    assert by_emp["002"] == 4
