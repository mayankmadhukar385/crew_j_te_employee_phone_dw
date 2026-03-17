"""Integration test — runs the full pipeline end-to-end with synthetic data.

Covers all routing paths:
  - EMERGENCY records
  - TEMP records
  - BASIC records (H, A, B home_away_ind values)
  - HOME schedule records
  - AWAY schedule records
  - ERROR records (invalid EMP_NBR)
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformations.away_phone_join import (
    assign_away_adjusted_priority,
    assign_away_scheduled_priority,
    compute_away_max_priority,
    join_away_max_to_basic,
    join_away_schedule_to_basic,
)
from src.transformations.away_priority_pipeline import build_away_schedule
from src.transformations.basic_phone_pipeline import route_basic_records, unpivot_basic_phones
from src.transformations.home_phone_join import (
    assign_home_adjusted_priority,
    assign_home_scheduled_priority,
    compute_home_max_priority,
    join_home_max_to_basic,
    join_home_schedule_to_basic,
)
from src.transformations.home_priority_pipeline import build_home_schedule
from src.transformations.phone_finalizer import finalize_phone_records
from src.transformations.phone_router import route_by_phone_type
from src.transformations.phone_union import union_all_phone_types
from tests.fixtures.sample_data import make_source_row


def _build_integration_dataset(spark: SparkSession):
    """Build a synthetic dataset covering all routing paths."""
    rows = [
        # Employee with EMERGENCY + BASIC (H) + HOME schedule
        make_source_row(
            EMP_NBR="111111111",
            PH_NBR="2125551001",
            BASIC_PH_NBR_1="2125559001",
            BASIC_PH_HOME_AWAY_CD_1="H",
            TELE_HOME_PRI_FROM_1="0800",
            TELE_HOME_PRI_TO_1="1700",
            TELE_HOME_PRI_SEQ_1_1="1",
        ),
        # Employee with TEMP phone
        make_source_row(
            EMP_NBR="222222222",
            PH_NBR=None,
            TEMP_PH_NBR="2125552001",
            TELE_TEMP_PH_DATE="20241231",
            TELE_TEMP_PH_TIME="235900",
        ),
        # Employee with BASIC (A) → AWAY routing
        make_source_row(
            EMP_NBR="333333333",
            PH_NBR=None,
            BASIC_PH_NBR_1="2125553001",
            BASIC_PH_HOME_AWAY_CD_1="A",
            TELE_AWAY_PRI_FROM_1="0900",
            TELE_AWAY_PRI_TO_1="1800",
            TELE_AWAY_PRI_SEQ_1_1="1",
        ),
        # Invalid EMP_NBR → ERROR
        make_source_row(EMP_NBR="INVALID", PH_NBR="2125554001"),
        # Employee with BASIC (B) → both HOME and AWAY routing
        make_source_row(
            EMP_NBR="444444444",
            PH_NBR="2125555001",
            BASIC_PH_NBR_1="2125555002",
            BASIC_PH_HOME_AWAY_CD_1="B",
        ),
    ]
    return spark.createDataFrame(rows)


def test_full_pipeline_end_to_end(spark, config):
    """Full pipeline executes without errors and produces non-empty final output."""
    source_df = _build_integration_dataset(spark)

    emergency_df, temp_df, basic_input_df, home_input_df, away_input_df, error_df = (
        route_by_phone_type(source_df, config)
    )

    all_basic_df, home_join_right_df, home_agg_right_df, away_join_right_df, away_agg_right_df = (
        route_basic_records(unpivot_basic_phones(basic_input_df))
    )

    home_sched_left_df = build_home_schedule(home_input_df)
    away_sched_left_df = build_away_schedule(away_input_df)

    home_joined_df = join_home_schedule_to_basic(home_sched_left_df, home_join_right_df)
    home_sched_out_df, home_cprty_df = assign_home_scheduled_priority(home_joined_df)
    home_max_df = compute_home_max_priority(home_cprty_df)
    home_basic_max_df = join_home_max_to_basic(home_max_df, home_agg_right_df)
    home_adj_df = assign_home_adjusted_priority(home_basic_max_df)

    away_joined_df = join_away_schedule_to_basic(away_sched_left_df, away_join_right_df)
    away_sched_out_df, away_cprty_df = assign_away_scheduled_priority(away_joined_df)
    away_max_df = compute_away_max_priority(away_cprty_df)
    away_basic_max_df = join_away_max_to_basic(away_max_df, away_agg_right_df)
    away_adj_df = assign_away_adjusted_priority(away_basic_max_df)

    all_df = union_all_phone_types(
        emergency_df, temp_df, all_basic_df,
        home_sched_out_df, home_adj_df,
        away_sched_out_df, away_adj_df,
    )

    final_df = finalize_phone_records(all_df)

    assert final_df.count() > 0
    assert error_df.count() == 1  # 1 row with EMP_NBR='INVALID'


def test_error_records_have_invalid_emp_nbr(spark, config):
    """Error records should all have non-numeric EMP_NBR values."""
    source_df = _build_integration_dataset(spark)
    _, _, _, _, _, error_df = route_by_phone_type(source_df, config)
    rows = error_df.collect()
    assert all(
        not str(r["EMP_NBR"]).strip().isdigit()
        for r in rows
    )


def test_final_output_has_correct_column_count(spark, config):
    """Final output has exactly 14 target columns."""
    source_df = _build_integration_dataset(spark)
    emergency_df, temp_df, basic_input_df, home_input_df, away_input_df, _ = (
        route_by_phone_type(source_df, config)
    )
    all_basic_df, home_join_right_df, home_agg_right_df, away_join_right_df, away_agg_right_df = (
        route_basic_records(unpivot_basic_phones(basic_input_df))
    )
    home_sched_left_df = build_home_schedule(home_input_df)
    away_sched_left_df = build_away_schedule(away_input_df)
    home_joined_df = join_home_schedule_to_basic(home_sched_left_df, home_join_right_df)
    home_sched_out_df, home_cprty_df = assign_home_scheduled_priority(home_joined_df)
    home_max_df = compute_home_max_priority(home_cprty_df)
    home_basic_max_df = join_home_max_to_basic(home_max_df, home_agg_right_df)
    home_adj_df = assign_home_adjusted_priority(home_basic_max_df)
    away_joined_df = join_away_schedule_to_basic(away_sched_left_df, away_join_right_df)
    away_sched_out_df, away_cprty_df = assign_away_scheduled_priority(away_joined_df)
    away_max_df = compute_away_max_priority(away_cprty_df)
    away_basic_max_df = join_away_max_to_basic(away_max_df, away_agg_right_df)
    away_adj_df = assign_away_adjusted_priority(away_basic_max_df)
    all_df = union_all_phone_types(
        emergency_df, temp_df, all_basic_df,
        home_sched_out_df, home_adj_df,
        away_sched_out_df, away_adj_df,
    )
    final_df = finalize_phone_records(all_df)
    assert len(final_df.columns) == 14
