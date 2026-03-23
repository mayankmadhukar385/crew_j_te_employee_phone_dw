"""Away priority schedule pipeline.

Implements: AWAY_CPY → AWAY_SEQ1/2/3_PVT → AWAY_SEQ2/3_TFM → AWAY_SEQ_FNL → INC_AWAY_PRTY_TFM

Identical logic to home_priority_pipeline.py. By the time the AWAY DataFrame reaches
this module, TELE_AWAY_PRI_* columns have already been renamed to TELE_HOME_PRI_* by
SEPARATE_PHTYPE_TFM (AWAY_TYPE_CPY link), so the same helper functions apply unchanged.
The only difference is CALL_LIST='AWAY'.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transformations.home_priority_pipeline import _unpivot_seq_group, _validate_time
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_away_schedule(df: DataFrame) -> DataFrame:
    """Build the away priority schedule DataFrame ready for AWAY_BASIC_JNR (left side).

    Implements AWAY_CPY + AWAY_SEQ1/2/3_PVT + AWAY_SEQ2/3_TFM + AWAY_SEQ_FNL + INC_AWAY_PRTY_TFM.

    Args:
        df: AWAY type DataFrame from phone_router (away_df). Columns are already renamed
            from TELE_AWAY_PRI_* to TELE_HOME_PRI_* by SEPARATE_PHTYPE_TFM.

    Returns:
        DataFrame with (EMP_NBR, CALL_LIST='AWAY', CALL_PRTY, LKP_CALL_PRTY,
        TELE_HOME_PRI_FROM, TELE_HOME_PRI_TO, UPD_TS, USER_ID).
    """
    # AWAY_SEQ1_PVT: group 1, CALL_PRTY 1–5 (no offset)
    seq1_df = _unpivot_seq_group(df, 1)

    # AWAY_SEQ2_PVT + AWAY_SEQ2_TFM: group 2, +5 offset → 6–10
    seq2_raw_df = _unpivot_seq_group(df, 2)
    seq2_df = seq2_raw_df.withColumn("CALL_PRTY", F.col("CALL_PRTY") + F.lit(5))

    # AWAY_SEQ3_PVT + AWAY_SEQ3_TFM: group 3, +10 offset → 11–15
    seq3_raw_df = _unpivot_seq_group(df, 3)
    seq3_df = seq3_raw_df.withColumn("CALL_PRTY", F.col("CALL_PRTY") + F.lit(10))

    # AWAY_SEQ_FNL: union all 3 groups
    away_seq_df = (
        seq1_df
        .unionByName(seq2_df, allowMissingColumns=True)
        .unionByName(seq3_df, allowMissingColumns=True)
    )

    # INC_AWAY_PRTY_TFM: identical to INC_PRTY_TFM but CALL_LIST='AWAY'
    result = (
        away_seq_df
        .filter(
            F.expr("try_cast(EMP_NBR as int) is not null") &
            F.expr("try_cast(LKP_CALL_PRTY as int) is not null")
        )
        .withColumn("CALL_PRTY", F.col("CALL_PRTY") + F.lit(1))
        .withColumn("CALL_LIST", F.lit("AWAY"))
        .withColumn("TELE_HOME_PRI_FROM", _validate_time("TELE_HOME_PRI_FROM"))
        .withColumn("TELE_HOME_PRI_TO", _validate_time("TELE_HOME_PRI_TO"))
    )
    return result
