"""Tests for phone_router.py (SEPARATE_PHTYPE_TFM)."""

import pytest
from pyspark.sql import SparkSession

from src.transformations.phone_router import route_by_phone_type
from tests.fixtures.sample_data import make_source_row


def _make_df(spark: SparkSession, rows: list[dict]):
    return spark.createDataFrame(rows)


def test_emergency_routing(spark, config):
    """Valid EMP_NBR + valid PH_NBR → row appears in emergency_df with correct constants."""
    row = make_source_row(EMP_NBR="123456789", PH_NBR="2125551234")
    df = _make_df(spark, [row])
    emergency_df, *_ = route_by_phone_type(df, config)
    result = emergency_df.collect()
    assert len(result) == 1
    r = result[0]
    assert r["CALL_LIST"] == "EMERGENCY"
    assert r["CALL_PRTY"] == 1
    assert r["EFF_START_TM"] == "0000"
    assert r["EFF_END_TM"] == "2359"


def test_error_routing_nonnumeric_emp_nbr(spark, config):
    """Non-numeric EMP_NBR → row in error_df ONLY; absent from all phone-type streams."""
    row = make_source_row(EMP_NBR="ABC", PH_NBR="2125551234")
    df = _make_df(spark, [row])
    emergency_df, temp_df, basic_df, home_df, away_df, error_df = route_by_phone_type(df, config)
    assert error_df.count() == 1
    assert emergency_df.count() == 0
    assert temp_df.count() == 0
    assert basic_df.count() == 0


def test_basic_routing_filters_invalid_phone(spark, config):
    """BASIC stream created; after unpivot only rows with valid PH_NBR reach ALL_BREC_OUT_TFM."""
    row = make_source_row(
        EMP_NBR="123456789",
        BASIC_PH_NBR_1="INVALID",
        BASIC_PH_NBR_2="2125551234",
    )
    df = _make_df(spark, [row])
    _, _, basic_df, _, _, _ = route_by_phone_type(df, config)
    # basic_df (BASIC_TYPE_TFM output) still has the row — filtering happens later in BASIC_REC_TFM
    assert basic_df.count() == 1


def test_null_handling_ph_comments_emergency(spark, config):
    """PH_COMMENTS with only whitespace → null in emergency output."""
    row = make_source_row(EMP_NBR="123456789", PH_NBR="2125551234", PH_COMMENTS="   ")
    df = _make_df(spark, [row])
    emergency_df, *_ = route_by_phone_type(df, config)
    r = emergency_df.collect()[0]
    assert r["PH_COMMENTS"] is None


def test_away_column_rename(spark, config):
    """AWAY_TYPE_CPY renames TELE_AWAY_PRI_FROM_1 → TELE_HOME_PRI_FROM_1."""
    row = make_source_row(EMP_NBR="123456789", TELE_AWAY_PRI_FROM_1="0800", TELE_AWAY_PRI_TO_1="1700")
    df = _make_df(spark, [row])
    _, _, _, _, away_df, _ = route_by_phone_type(df, config)
    assert "TELE_HOME_PRI_FROM_1" in away_df.columns
    assert "TELE_AWAY_PRI_FROM_1" not in away_df.columns
    r = away_df.collect()[0]
    assert r["TELE_HOME_PRI_FROM_1"] == "0800"


def test_multicast_single_row_in_multiple_streams(spark, config):
    """A valid employee with PH_NBR → appears in both emergency_df and basic_df (multi-cast)."""
    row = make_source_row(EMP_NBR="123456789", PH_NBR="2125551234", BASIC_PH_NBR_1="2125559999")
    df = _make_df(spark, [row])
    emergency_df, _, basic_df, _, _, _ = route_by_phone_type(df, config)
    assert emergency_df.count() == 1
    assert basic_df.count() == 1
