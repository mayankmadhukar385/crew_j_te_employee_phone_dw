"""Home phone join pipeline.

Implements: HOME_BASIC_JNR → NEW_PRTY_TFM → MAX_PRTY_AGG → BASIC_MAX_JNR → ADJUSTING_PRTY_TFM

Pattern:
  1. Inner join schedule (left) to basic phone records (right) on EMP_NBR + LKP_CALL_PRTY
  2. Assign sequential CALL_PRTY per employee using Window row_number (NEW_PRTY_TFM)
  3. Aggregate MAX(CALL_PRTY) per employee (MAX_PRTY_AGG)
  4. Inner join MAX back to basic records on EMP_NBR (BASIC_MAX_JNR)
  5. Assign adjusted CALL_PRTY = MAX_CALL_PRTY + row_number per employee (ADJUSTING_PRTY_TFM)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.logger import get_logger

logger = get_logger(__name__)


def join_home_schedule_to_basic(sched_df: DataFrame, basic_df: DataFrame) -> DataFrame:
    """Inner join home schedule rows to basic phone records (HOME_BASIC_JNR).

    Left:  schedule rows from INC_PRTY_TFM (CALL_PRIORITY_LEFT_JNR)
    Right: basic phone rows from BASIC_REC_TFM (CALL_PRIORITY_RIGHT_JNR)
    Keys:  EMP_NBR, LKP_CALL_PRTY

    Output columns come from the left (schedule) side.

    Args:
        sched_df: Home schedule DataFrame from build_home_schedule.
        basic_df: Basic phone right-side DataFrame (home_join_right_df from route_basic_records).

    Returns:
        Joined DataFrame with schedule window attributes and phone attributes from the left side.
    """
    joined = sched_df.alias("left").join(
        basic_df.alias("right"),
        on=["EMP_NBR", "LKP_CALL_PRTY"],
        how="inner",
    )
    # Select all columns from left side; pick phone attributes from right side
    left_cols = [
        F.col(f"left.{c}").alias(c)
        for c in sched_df.columns
        if c not in ("LKP_CALL_PRTY",)
    ]
    right_phone_cols = [
        F.col(f"right.{c}").alias(c)
        for c in ("PH_NBR", "PH_ACCESS", "PH_COMMENTS", "PH_TYPE", "UNLISTD_IND", "HOME_AWAY_IND", "TEMP_PH_EXP_TS")
        if c in basic_df.columns
    ]
    return joined.select(*left_cols, *right_phone_cols)


def assign_home_scheduled_priority(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Assign sequential CALL_PRTY per employee (NEW_PRTY_TFM).

    Uses Window row_number partitioned by EMP_NBR to replicate the DataStage stage
    variable pattern (svCalcNew = if emp changed then 1 else svCalcOld+1).
    Input must be pre-sorted by EMP_NBR.

    Args:
        df: Joined DataFrame from join_home_schedule_to_basic.

    Returns:
        Tuple (to_funnel_df, to_agg_df):
          - to_funnel_df: NPRTY_OUT_TFM — goes to ALL_REC_FNL; has EFF_START/END_TM
          - to_agg_df: CPRTY_OUT_TFM — goes to MAX_PRTY_AGG; only EMP_NBR + CALL_PRTY
    """
    w = Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY")
    with_prty = (
        df.withColumn("CALL_PRTY", F.row_number().over(w))
        .withColumn("EFF_START_TM", F.col("TELE_HOME_PRI_FROM"))
        .withColumn("EFF_END_TM", F.col("TELE_HOME_PRI_TO"))
    )
    to_funnel_df = with_prty.drop("TELE_HOME_PRI_FROM", "TELE_HOME_PRI_TO", "LKP_CALL_PRTY")
    to_agg_df = with_prty.select("EMP_NBR", "CALL_PRTY")
    return to_funnel_df, to_agg_df


def compute_home_max_priority(df: DataFrame) -> DataFrame:
    """Compute MAX(CALL_PRTY) per employee (MAX_PRTY_AGG).

    Args:
        df: CPRTY_OUT_TFM DataFrame (EMP_NBR, CALL_PRTY) from assign_home_scheduled_priority.

    Returns:
        DataFrame with (EMP_NBR, MAX_CALL_PRTY).
    """
    return df.groupBy("EMP_NBR").agg(F.max("CALL_PRTY").alias("MAX_CALL_PRTY"))


def join_home_max_to_basic(max_df: DataFrame, basic_df: DataFrame) -> DataFrame:
    """Join MAX_CALL_PRTY back to basic phone records (BASIC_MAX_JNR).

    Enriches unscheduled BASIC HOME phone records with the maximum scheduled priority,
    so ADJUSTING_PRTY_TFM can start numbering after the scheduled entries.

    Args:
        max_df: DataFrame with (EMP_NBR, MAX_CALL_PRTY) from compute_home_max_priority.
        basic_df: home_agg_right_df from route_basic_records (BREC_OUT_TFM link).

    Returns:
        Enriched DataFrame with all basic_df columns plus MAX_CALL_PRTY.
    """
    return max_df.join(basic_df, on="EMP_NBR", how="inner")


def assign_home_adjusted_priority(df: DataFrame) -> DataFrame:
    """Assign CALL_PRTY = MAX_CALL_PRTY + row_number per employee (ADJUSTING_PRTY_TFM).

    For BASIC HOME records that have no schedule window, this ensures their priorities
    start immediately after the last scheduled priority for the same employee.

    Args:
        df: DataFrame from join_home_max_to_basic (contains MAX_CALL_PRTY column).

    Returns:
        DataFrame with adjusted CALL_PRTY, CALL_LIST='HOME', EFF_START_TM='0001', EFF_END_TM='2359'.
    """
    w = Window.partitionBy("EMP_NBR").orderBy("CALL_PRTY")
    return (
        df.withColumn("CALL_PRTY", F.col("MAX_CALL_PRTY") + F.row_number().over(w))
        .withColumn("CALL_LIST", F.lit("HOME"))
        .withColumn("EFF_START_TM", F.lit("0001"))
        .withColumn("EFF_END_TM", F.lit("2359"))
        .drop("MAX_CALL_PRTY", "LKP_CALL_PRTY")
    )
