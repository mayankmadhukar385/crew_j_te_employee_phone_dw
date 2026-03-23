"""Tests for basic_phone_pipeline.py (BASIC_TYPE_PVT + BASIC_REC_TFM)."""

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from src.transformations.basic_phone_pipeline import route_basic_records, unpivot_basic_phones
from src.transformations.phone_router import route_by_phone_type
from tests.fixtures.sample_data import make_source_row

def _df_from_row(spark, row):
    schema = StructType([
        StructField(col, StringType(), True) for col in row.keys()
    ])
    return spark.createDataFrame([row], schema=schema)

def _basic_df(spark: SparkSession, rows: list[dict]):
    source_df = spark.createDataFrame(rows)
    _, _, basic_df, _, _, _ = route_by_phone_type(source_df, {})
    return basic_df


def test_unpivot_produces_5_rows(spark):
    """1 row with all 5 BASIC phone slots → 5 output rows with CALL_PRTY=1..5."""
    row = make_source_row(
        EMP_NBR="111111111",
        BASIC_PH_NBR_1="2125551001",
        BASIC_PH_NBR_2="2125551002",
        BASIC_PH_NBR_3="2125551003",
        BASIC_PH_NBR_4="2125551004",
        BASIC_PH_NBR_5="2125551005",
    )
    df = _df_from_row(spark, row)
    _, _, basic_input_df, _, _, _ = route_by_phone_type(df, {})
    result = unpivot_basic_phones(basic_input_df)
    assert result.count() == 5
    priorities = {r["CALL_PRTY"] for r in result.collect()}
    assert priorities == {1, 2, 3, 4, 5}


def test_unpivot_null_phone_rows_excluded_by_basic_rec_tfm(spark):
    """Only BASIC_PH_NBR_1 populated → after ALL_BREC_OUT_TFM filter, only 1 row survives."""
    row = make_source_row(EMP_NBR="111111111", BASIC_PH_NBR_1="2125551001")
    df = _df_from_row(spark, row)
    _, _, basic_input_df, _, _, _ = route_by_phone_type(df, {})
    pivoted = unpivot_basic_phones(basic_input_df)
    all_basic_df, *_ = route_basic_records(pivoted)
    assert all_basic_df.count() == 1


def test_home_away_routing_both(spark):
    """HOME_AWAY_IND='B' → row appears in BOTH home join feed AND away join feed."""
    row = make_source_row(
        EMP_NBR="111111111",
        BASIC_PH_NBR_1="2125551001",
        BASIC_PH_HOME_AWAY_CD_1="B",
    )
    df = _df_from_row(spark, row)
    _, _, basic_input_df, _, _, _ = route_by_phone_type(df, {})
    pivoted = unpivot_basic_phones(basic_input_df)
    _, home_join_right_df, _, away_join_right_df, _ = route_basic_records(pivoted)
    assert home_join_right_df.count() >= 1
    assert away_join_right_df.count() >= 1


def test_call_prty_incremented_by_1(spark):
    """BASIC_REC_TFM applies CALL_PRTY+1: slot 1 → CALL_PRTY=2."""
    row = make_source_row(EMP_NBR="111111111", BASIC_PH_NBR_1="2125551001")
    df = _df_from_row(spark, row)
    _, _, basic_input_df, _, _, _ = route_by_phone_type(df, {})
    pivoted = unpivot_basic_phones(basic_input_df)
    all_basic_df, *_ = route_basic_records(pivoted)
    r = all_basic_df.filter(F.col("PH_NBR") == "2125551001").collect()[0]
    assert r["CALL_PRTY"] == 2  # slot 1 → unpivot CALL_PRTY=1 → +1 = 2
