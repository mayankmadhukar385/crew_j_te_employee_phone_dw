"""Tests for home_priority_pipeline.py (HOME_CPY + HOME_SEQ*_PVT + TFM + FNL + INC_PRTY_TFM)."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from src.transformations.home_priority_pipeline import build_home_schedule
from src.transformations.phone_router import route_by_phone_type
from tests.fixtures.sample_data import make_source_row


def _home_df(spark, overrides):
    row = make_source_row(**overrides)
    schema = StructType([
        StructField(col, StringType(), True) for col in row.keys()
    ])
    df = spark.createDataFrame([row], schema=schema)
    _, home_df, _, _, _, _ = route_by_phone_type(df, {})
    return home_df


def test_pivot_3_groups_produce_15_rows(spark):
    """1 employee with all 3 HOME SEQ groups filled (5 seq each) → 15 rows."""
    overrides = {}
    for g in range(1, 4):
        overrides[f"TELE_HOME_PRI_FROM_{g}"] = "0800"
        overrides[f"TELE_HOME_PRI_TO_{g}"] = "1700"
        for s in range(1, 6):
            overrides[f"TELE_HOME_PRI_SEQ_{s}_{g}"] = str(s)
    home_df = _home_df(spark, overrides)
    result = build_home_schedule(home_df)
    assert result.count() == 15


def test_call_prty_offset_group2(spark):
    """After HOME_SEQ2_TFM, row with base CALL_PRTY=3 (slot 3) gets +5 offset → CALL_PRTY=8, then +1 from INC_PRTY_TFM → 9."""
    overrides = {
        "TELE_HOME_PRI_FROM_2": "0800",
        "TELE_HOME_PRI_TO_2": "1700",
        "TELE_HOME_PRI_SEQ_3_2": "5",  # slot 3 of group 2 → CALL_PRTY=3 base
    }
    home_df = _home_df(spark, overrides)
    result = build_home_schedule(home_df)
    # group 2, slot 3: CALL_PRTY = 3 + 5 (offset) + 1 (INC_PRTY_TFM) = 9
    priorities = {r["CALL_PRTY"] for r in result.collect()}
    assert 9 in priorities


def test_inc_prty_tfm_invalid_time_nulled(spark):
    """TELE_HOME_PRI_FROM='9999' (invalid) → null after INC_PRTY_TFM time validation."""
    overrides = {
        "TELE_HOME_PRI_FROM_1": "9999",
        "TELE_HOME_PRI_TO_1": "1700",
        "TELE_HOME_PRI_SEQ_1_1": "3",
    }
    home_df = _home_df(spark, overrides)
    result = build_home_schedule(home_df)
    rows = result.collect()
    assert len(rows) > 0
    r = rows[0]
    assert r["TELE_HOME_PRI_FROM"] is None


def test_inc_prty_tfm_valid_time_passes(spark):
    """TELE_HOME_PRI_FROM='0800' (valid HH:MM when padded) → passes validation unchanged."""
    overrides = {
        "TELE_HOME_PRI_FROM_1": "0800",
        "TELE_HOME_PRI_TO_1": "1700",
        "TELE_HOME_PRI_SEQ_1_1": "2",
    }
    home_df = _home_df(spark, overrides)
    result = build_home_schedule(home_df)
    rows = result.collect()
    assert len(rows) > 0
    r = rows[0]
    assert r["TELE_HOME_PRI_FROM"] == "0800"


def test_call_list_assigned_home(spark):
    """All rows from build_home_schedule have CALL_LIST='HOME'."""
    overrides = {
        "TELE_HOME_PRI_FROM_1": "0800",
        "TELE_HOME_PRI_TO_1": "1700",
        "TELE_HOME_PRI_SEQ_1_1": "1",
    }
    home_df = _home_df(spark, overrides)
    result = build_home_schedule(home_df)
    distinct_call_lists = {r["CALL_LIST"] for r in result.collect()}
    assert distinct_call_lists == {"HOME"}
