"""Basic phone pipeline — implements BASIC_TYPE_PVT + BASIC_REC_TFM.

BASIC_TYPE_PVT: Unpivots 5 BASIC phone slot columns into vertical rows (one per slot).
BASIC_REC_TFM:  Applies CALL_PRTY+1, then fans out to 5 downstream consumers based
                on HOME_AWAY_IND routing filters.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


def unpivot_basic_phones(df: DataFrame) -> DataFrame:
    """Unpivot 5 BASIC phone slot columns into one row per slot (BASIC_TYPE_PVT).

    Produces a row for each of the 5 BASIC phone slots. CALL_PRTY = slot index (1–5).
    Passthrough: EMP_NBR, CALL_LIST, EFF_START_TM, EFF_END_TM, USER_ID, UPD_TS.

    Args:
        df: DataFrame from BASIC_TYPE_TFM output (basic_df from phone_router).

    Returns:
        Unpivoted DataFrame with CALL_PRTY column (1–5) and canonical PH_* columns.
    """
    return df.select(
        "EMP_NBR",
        "CALL_LIST",
        "EFF_START_TM",
        "EFF_END_TM",
        "USER_ID",
        "UPD_TS",
        F.expr("""
            stack(5,
              1, BASIC_PH_NBR_1, BASIC_PH_ACCESS_1, BASIC_PH_COMMENT_1, BASIC_PH_TYPE_1,
                 BASIC_PH_UNLIST_CD_1, BASIC_PH_HOME_AWAY_CD_1, TEMP_PH_EXP_TS_1,
              2, BASIC_PH_NBR_2, BASIC_PH_ACCESS_2, BASIC_PH_COMMENT_2, BASIC_PH_TYPE_2,
                 BASIC_PH_UNLIST_CD_2, BASIC_PH_HOME_AWAY_CD_2, TEMP_PH_EXP_TS_2,
              3, BASIC_PH_NBR_3, BASIC_PH_ACCESS_3, BASIC_PH_COMMENT_3, BASIC_PH_TYPE_3,
                 BASIC_PH_UNLIST_CD_3, BASIC_PH_HOME_AWAY_CD_3, TEMP_PH_EXP_TS_3,
              4, BASIC_PH_NBR_4, BASIC_PH_ACCESS_4, BASIC_PH_COMMENT_4, BASIC_PH_TYPE_4,
                 BASIC_PH_UNLIST_CD_4, BASIC_PH_HOME_AWAY_CD_4, TEMP_PH_EXP_TS_4,
              5, BASIC_PH_NBR_5, BASIC_PH_ACCESS_5, BASIC_PH_COMMENT_5, BASIC_PH_TYPE_5,
                 BASIC_PH_UNLIST_CD_5, BASIC_PH_HOME_AWAY_CD_5, TEMP_PH_EXP_TS_5
            ) AS (CALL_PRTY, PH_NBR, PH_ACCESS, PH_COMMENTS, PH_TYPE,
                  UNLISTD_IND, HOME_AWAY_IND, TEMP_PH_EXP_TS)
        """),
    )


def route_basic_records(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """Apply CALL_PRTY+1 and fan out to 5 consumers (BASIC_REC_TFM).

    ALL_BREC_OUT_TFM:        direct to ALL_REC_FNL — valid phone numbers
    CALL_PRIORITY_RIGHT_JNR: HOME join right side — HOME/BOTH records
    BREC_OUT_TFM:            HOME aggregation right side — HOME/BOTH records
    CALL_PRI_RIGHT_JNR:      AWAY join right side — AWAY/BOTH records
    BASIC_REC_OUT_TFM:       AWAY aggregation right side — AWAY/BOTH records

    Args:
        df: DataFrame from unpivot_basic_phones (BREC_OUT_PVT schema).

    Returns:
        Tuple (all_basic_df, home_join_right_df, home_agg_right_df, away_join_right_df, away_agg_right_df).
    """
    # Materialise with CALL_PRTY+1 applied once; reused across all 5 outputs
    incremented = df.withColumn("CALL_PRTY", F.col("CALL_PRTY") + F.lit(1))

    valid_phone = F.col("PH_NBR").cast("long").isNotNull()
    home_or_both = (
        valid_phone
        & F.col("HOME_AWAY_IND").isNotNull()
        & F.col("HOME_AWAY_IND").isin("B", "H")
    )
    away_or_both = (
        valid_phone
        & F.col("HOME_AWAY_IND").isNotNull()
        & F.col("HOME_AWAY_IND").isin("B", "A")
    )

    # ALL_BREC_OUT_TFM — filter: isvalid("int64", PH_NBR)
    all_basic_df = incremented.filter(valid_phone)

    # CALL_PRIORITY_RIGHT_JNR — right side of HOME_BASIC_JNR join
    # Adds LKP_CALL_PRTY = CALL_PRTY (same value, used as join key)
    home_join_right_df = incremented.filter(home_or_both).withColumn(
        "LKP_CALL_PRTY", F.col("CALL_PRTY")
    )

    # BREC_OUT_TFM — right side of BASIC_MAX_JNR (same filter as home_join_right)
    home_agg_right_df = incremented.filter(home_or_both)

    # CALL_PRI_RIGHT_JNR — right side of AWAY_BASIC_JNR join
    away_join_right_df = incremented.filter(away_or_both).withColumn(
        "LKP_CALL_PRTY", F.col("CALL_PRTY")
    )

    # BASIC_REC_OUT_TFM — right side of AWAY_MAX_JNR
    away_agg_right_df = incremented.filter(away_or_both)

    return all_basic_df, home_join_right_df, home_agg_right_df, away_join_right_df, away_agg_right_df
