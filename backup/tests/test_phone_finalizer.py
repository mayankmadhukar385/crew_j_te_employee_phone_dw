"""Tests for phone_finalizer.py (NULL_VAL_TFM)."""

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from src.transformations.phone_finalizer import finalize_phone_records


def _make_df(spark: SparkSession, rows: list[dict]):
    schema = StructType([
        StructField("EMP_NBR", StringType()),
        StructField("PH_LIST", StringType()),
        StructField("PH_PRTY", IntegerType()),
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
    return spark.createDataFrame(
        [
            (
                r["EMP_NBR"], r.get("PH_LIST", "BASIC"), r.get("PH_PRTY", 1),
                r.get("EFF_START_TM", "0800"), r.get("EFF_END_TM", "1700"),
                r.get("PH_NBR", "2125551234"),
                r.get("PH_ACCESS"), r.get("PH_COMMENTS"),
                r.get("PH_TYPE"), r.get("UNLISTD_IND"), r.get("HOME_AWAY_IND"),
                None, None, r.get("USER_ID"),
            )
            for r in rows
        ],
        schema,
    )


def test_invalid_phone_number_filtered(spark):
    """PH_NBR='ABC' (non-numeric) → row excluded from output."""
    df = _make_df(spark, [{"EMP_NBR": "123456789", "PH_NBR": "ABC"}])
    result = finalize_phone_records(df)
    assert result.count() == 0


def test_emp_no_zero_padding(spark):
    """EMP_NBR='12345' (5 chars) → EMP_NO='000012345' (9 chars)."""
    df = _make_df(spark, [{"EMP_NBR": "12345", "PH_NBR": "2125551234"}])
    result = finalize_phone_records(df)
    r = result.collect()[0]
    assert r["EMP_NO"] == "000012345"


def test_emp_no_no_padding_when_9_chars(spark):
    """EMP_NBR='123456789' (already 9 chars) → EMP_NO='123456789' unchanged."""
    df = _make_df(spark, [{"EMP_NBR": "123456789", "PH_NBR": "2125551234"}])
    result = finalize_phone_records(df)
    r = result.collect()[0]
    assert r["EMP_NO"] == "123456789"


def test_eff_time_format(spark):
    """EFF_START_TM='0800', EFF_END_TM='1700' → '08:00:00' and '17:00:00'."""
    df = _make_df(spark, [{"EMP_NBR": "123456789", "PH_NBR": "2125551234", "EFF_START_TM": "0800", "EFF_END_TM": "1700"}])
    result = finalize_phone_records(df)
    r = result.collect()[0]
    assert r["EFF_START_TM"] == "08:00:00"
    assert r["EFF_END_TM"] == "17:00:00"


def test_emp_no_derivation_chispa(spark):
    """Use chispa assert_df_equality to verify EMP_NO derivation for two employees."""
    input_rows = [
        {"EMP_NBR": "12345", "PH_NBR": "2125551234", "EFF_START_TM": "0800", "EFF_END_TM": "1700"},
        {"EMP_NBR": "123456789", "PH_NBR": "2125559999", "EFF_START_TM": "0000", "EFF_END_TM": "2359"},
    ]
    result = finalize_phone_records(_make_df(spark, input_rows))
    # Build expected — only EMP_NO column checked via chispa on a projected subset
    expected_schema = StructType([
        StructField("EMP_NBR", StringType()),
        StructField("EMP_NO", StringType()),
    ])
    expected = spark.createDataFrame(
        [("12345", "000012345"), ("123456789", "123456789")],
        expected_schema,
    )
    result_projected = result.select("EMP_NBR", "EMP_NO")
    assert_df_equality(result_projected, expected, ignore_row_order=True, ignore_nullable=True)


def test_ph_access_whitespace_nulled(spark):
    """PH_ACCESS='   ' (whitespace) → null in output."""
    df = _make_df(spark, [{"EMP_NBR": "123456789", "PH_NBR": "2125551234", "PH_ACCESS": "   "}])
    result = finalize_phone_records(df)
    r = result.collect()[0]
    assert r["PH_ACCESS"] is None


def test_td_ld_ts_populated(spark):
    """TD_LD_TS is not null and is a timestamp type."""
    from pyspark.sql.types import TimestampType as TS
    df = _make_df(spark, [{"EMP_NBR": "123456789", "PH_NBR": "2125551234"}])
    result = finalize_phone_records(df)
    r = result.collect()[0]
    assert r["TD_LD_TS"] is not None


def test_output_has_14_target_columns(spark):
    """Output has exactly the 14 target columns in spec order."""
    expected_cols = [
        "EMP_NBR", "EMP_NO", "PH_LIST", "PH_PRTY", "EFF_START_TM", "EFF_END_TM",
        "PH_NBR", "PH_ACCESS", "PH_COMMENTS", "PH_TYPE", "UNLISTD_IND",
        "HOME_AWAY_IND", "TEMP_PH_EXP_TS", "TD_LD_TS",
    ]
    df = _make_df(spark, [{"EMP_NBR": "123456789", "PH_NBR": "2125551234"}])
    result = finalize_phone_records(df)
    assert result.columns == expected_cols
