"""Tests for phone_union.py (ALL_REC_FNL)."""

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from src.transformations.phone_union import union_all_phone_types


def _minimal_df(spark: SparkSession, call_list: str, n: int = 1):
    """Create a minimal phone DataFrame with CALL_LIST and CALL_PRTY set."""
    schema = StructType([
        StructField("EMP_NBR", StringType()),
        StructField("CALL_LIST", StringType()),
        StructField("CALL_PRTY", IntegerType()),
        StructField("EFF_START_TM", StringType()),
        StructField("EFF_END_TM", StringType()),
        StructField("PH_NBR", StringType()),
        StructField("PH_ACCESS", StringType()),
        StructField("PH_COMMENTS", StringType()),
        StructField("PH_TYPE", StringType()),
        StructField("UNLISTD_IND", StringType()),
        StructField("HOME_AWAY_IND", StringType()),
        StructField("TEMP_PH_EXP_TS", TimestampType()),
        StructField("UPD_TS", TimestampType()),
        StructField("USER_ID", StringType()),
    ])
    rows = [
        (f"00{i}", call_list, i, "0000", "2359", "2125551234", None, None, None, None, None, None, None, "USR")
        for i in range(1, n + 1)
    ]
    return spark.createDataFrame(rows, schema)


def test_all_7_streams_unioned(spark):
    """1 row in each of 7 streams → 7 rows in output."""
    result = union_all_phone_types(
        _minimal_df(spark, "EMERGENCY"),
        _minimal_df(spark, "TEMP"),
        _minimal_df(spark, "BASIC"),
        _minimal_df(spark, "HOME"),
        _minimal_df(spark, "HOME"),
        _minimal_df(spark, "AWAY"),
        _minimal_df(spark, "AWAY"),
    )
    assert result.count() == 7


def test_column_rename_call_list_to_ph_list(spark):
    """After union, CALL_LIST is renamed to PH_LIST and CALL_PRTY to PH_PRTY."""
    df = _minimal_df(spark, "HOME")
    result = union_all_phone_types(df, df, df, df, df, df, df)
    assert "PH_LIST" in result.columns
    assert "PH_PRTY" in result.columns
    assert "CALL_LIST" not in result.columns
    assert "CALL_PRTY" not in result.columns


def test_union_output_chispa(spark):
    """Use chispa assert_df_equality to verify union produces correct PH_LIST values."""
    emergency_df = _minimal_df(spark, "EMERGENCY")
    temp_df = _minimal_df(spark, "TEMP")
    result = union_all_phone_types(
        emergency_df, temp_df,
        _minimal_df(spark, "BASIC"),
        _minimal_df(spark, "HOME"),
        _minimal_df(spark, "HOME"),
        _minimal_df(spark, "AWAY"),
        _minimal_df(spark, "AWAY"),
    )
    expected_schema = StructType([
        StructField("EMP_NBR", StringType()),
        StructField("PH_LIST", StringType()),
    ])
    # Emergency row has EMP_NBR="001", Temp has "001" too but from _minimal_df with n=1 each
    expected = spark.createDataFrame(
        [("001", "EMERGENCY"), ("001", "TEMP"), ("001", "BASIC"),
         ("001", "HOME"), ("001", "HOME"), ("001", "AWAY"), ("001", "AWAY")],
        expected_schema,
    )
    result_projected = result.select("EMP_NBR", "PH_LIST")
    assert_df_equality(result_projected, expected, ignore_row_order=True, ignore_nullable=True)


def test_call_list_values_preserved(spark):
    """All 5 CALL_LIST values survive union as PH_LIST values."""
    result = union_all_phone_types(
        _minimal_df(spark, "EMERGENCY"),
        _minimal_df(spark, "TEMP"),
        _minimal_df(spark, "BASIC"),
        _minimal_df(spark, "HOME"),
        _minimal_df(spark, "HOME"),
        _minimal_df(spark, "AWAY"),
        _minimal_df(spark, "AWAY"),
    )
    ph_lists = {r["PH_LIST"] for r in result.collect()}
    assert ph_lists == {"EMERGENCY", "TEMP", "BASIC", "HOME", "AWAY"}
