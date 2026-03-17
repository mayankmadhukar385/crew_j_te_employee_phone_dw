"""Away phone join pipeline.

Implements: AWAY_BASIC_JNR → ABREC_NEW_PRTY_TFM → MAX_ABPRTY_AGG → AWAY_MAX_JNR → ADJUST_PRTY_TFM

Identical structure to home_phone_join.py, operating on the AWAY path.
CALL_LIST is set to 'AWAY' instead of 'HOME'.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transformations.home_phone_join import (
    assign_home_adjusted_priority,
    assign_home_scheduled_priority,
    compute_home_max_priority,
    join_home_max_to_basic,
    join_home_schedule_to_basic,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


def join_away_schedule_to_basic(sched_df: DataFrame, basic_df: DataFrame) -> DataFrame:
    """Inner join away schedule rows to basic phone records (AWAY_BASIC_JNR).

    Args:
        sched_df: Away schedule DataFrame from build_away_schedule.
        basic_df: Basic phone right-side DataFrame (away_join_right_df from route_basic_records).

    Returns:
        Joined DataFrame — same structure as HOME equivalent.
    """
    return join_home_schedule_to_basic(sched_df, basic_df)


def assign_away_scheduled_priority(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Assign sequential CALL_PRTY per employee for AWAY records (ABREC_NEW_PRTY_TFM).

    Args:
        df: Joined DataFrame from join_away_schedule_to_basic.

    Returns:
        Tuple (to_funnel_df, to_agg_df) — same semantics as HOME equivalent.
    """
    return assign_home_scheduled_priority(df)


def compute_away_max_priority(df: DataFrame) -> DataFrame:
    """Compute MAX(CALL_PRTY) per employee for AWAY records (MAX_ABPRTY_AGG).

    Args:
        df: ABCPRTY_OUT_TFM DataFrame from assign_away_scheduled_priority.

    Returns:
        DataFrame with (EMP_NBR, MAX_CALL_PRTY).
    """
    return compute_home_max_priority(df)


def join_away_max_to_basic(max_df: DataFrame, basic_df: DataFrame) -> DataFrame:
    """Join MAX_CALL_PRTY back to basic AWAY phone records (AWAY_MAX_JNR / DSLink85).

    Args:
        max_df: DataFrame with (EMP_NBR, MAX_CALL_PRTY) from compute_away_max_priority.
        basic_df: away_agg_right_df from route_basic_records (BASIC_REC_OUT_TFM link).

    Returns:
        Enriched DataFrame with all basic_df columns plus MAX_CALL_PRTY.
    """
    return join_home_max_to_basic(max_df, basic_df)


def assign_away_adjusted_priority(df: DataFrame) -> DataFrame:
    """Assign CALL_PRTY = MAX_CALL_PRTY + row_number for AWAY records (ADJUST_PRTY_TFM).

    Args:
        df: DataFrame from join_away_max_to_basic.

    Returns:
        DataFrame with adjusted CALL_PRTY, CALL_LIST='AWAY', EFF_START_TM='0001', EFF_END_TM='2359'.
    """
    result = assign_home_adjusted_priority(df)
    return result.withColumn("CALL_LIST", F.lit("AWAY"))
