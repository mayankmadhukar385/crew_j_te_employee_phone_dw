"""Phone record union — implements ALL_REC_FNL (PxFunnel).

Unions all 7 phone-type streams and renames CALL_LIST → PH_LIST and CALL_PRTY → PH_PRTY.
The rename happens exactly here — not earlier, not later.
"""

from pyspark.sql import DataFrame
from src.utils.logger import get_logger

logger = get_logger(__name__)


def union_all_phone_types(
    emergency_df: DataFrame,
    temp_df: DataFrame,
    basic_df: DataFrame,
    home_sched_df: DataFrame,
    home_adj_df: DataFrame,
    away_sched_df: DataFrame,
    away_adj_df: DataFrame,
) -> DataFrame:
    """Union all 7 phone-type streams (ALL_REC_FNL) and rename list/priority columns.

    Implements ALL_REC_FNL: 7-way unionByName followed by
    CALL_LIST → PH_LIST and CALL_PRTY → PH_PRTY column renames.

    Args:
        emergency_df: Emergency phone records.
        temp_df: Temp phone records.
        basic_df: Basic phone records (all_basic_df from route_basic_records).
        home_sched_df: Scheduled HOME phone records (to_funnel from assign_home_scheduled_priority).
        home_adj_df: Adjusted HOME phone records (from assign_home_adjusted_priority).
        away_sched_df: Scheduled AWAY phone records.
        away_adj_df: Adjusted AWAY phone records.

    Returns:
        Unified DataFrame with PH_LIST and PH_PRTY columns (CALL_LIST/CALL_PRTY renamed).
    """
    all_df = (
        emergency_df
        .unionByName(temp_df, allowMissingColumns=True)
        .unionByName(basic_df, allowMissingColumns=True)
        .unionByName(home_sched_df, allowMissingColumns=True)
        .unionByName(home_adj_df, allowMissingColumns=True)
        .unionByName(away_sched_df, allowMissingColumns=True)
        .unionByName(away_adj_df, allowMissingColumns=True)
        .withColumnRenamed("CALL_LIST", "PH_LIST")
        .withColumnRenamed("CALL_PRTY", "PH_PRTY")
    )
    return all_df
