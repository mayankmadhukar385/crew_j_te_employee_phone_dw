"""Home priority schedule pipeline.

Implements: HOME_CPY → HOME_SEQ1/2/3_PVT → HOME_SEQ2/3_TFM → HOME_SEQ_FNL → INC_PRTY_TFM

Fan-out fan-in pattern:
  1. Fan out to 3 groups (HOME_CPY)
  2. Unpivot each group (5 SEQ slots → 5 rows each)  → 15 rows max per employee
  3. Apply CALL_PRTY offsets (+0, +5, +10) for groups 2/3
  4. Union all 3 groups (HOME_SEQ_FNL)
  5. Validate time strings and assign CALL_LIST='HOME' (INC_PRTY_TFM)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _unpivot_seq_group(df: DataFrame, group: int) -> DataFrame:
    """Unpivot one HOME priority sequence group into 5 rows.

    Args:
        df: DataFrame projected to the specified group's columns.
        group: Group number (1, 2, or 3).

    Returns:
        DataFrame with columns (EMP_NBR, TELE_HOME_PRI_FROM, TELE_HOME_PRI_TO,
        CALL_PRTY, LKP_CALL_PRTY, UPD_TS, USER_ID).
    """
    from_col = f"TELE_HOME_PRI_FROM_{group}"
    to_col = f"TELE_HOME_PRI_TO_{group}"
    seq_cols = [f"TELE_HOME_PRI_SEQ_{s}_{group}" for s in range(1, 6)]

    projected = df.select(
        "EMP_NBR",
        "UPD_TS",
        "USER_ID",
        F.col(from_col).alias("TELE_HOME_PRI_FROM"),
        F.col(to_col).alias("TELE_HOME_PRI_TO"),
        *[F.col(c) for c in seq_cols],
    )

    stack_expr = ", ".join(
        f"{i}, {seq_cols[i - 1]}" for i in range(1, 6)
    )
    return projected.select(
        "EMP_NBR",
        "TELE_HOME_PRI_FROM",
        "TELE_HOME_PRI_TO",
        "UPD_TS",
        "USER_ID",
        F.expr(f"stack(5, {stack_expr}) AS (CALL_PRTY, LKP_CALL_PRTY)"),
    )


def _validate_time(col_name: str) -> F.Column:
    """Return null if HHMM string is not a valid time, else trim and return value.

    Implements DataStage INC_PRTY_TFM verbatim:
    if not(isvalid("time", left(X,2):':':right(X,2):':00')) then setnull()
    else trimleadingtrailing(X)

    Args:
        col_name: Column name containing a 4-character HHMM time string.

    Returns:
        Column expression — null if invalid, trimmed string if valid.
    """
    formatted = F.concat(
    F.substring(F.col(col_name), 1, 2),
    F.lit(":"),
    F.substring(F.col(col_name), 3, 2),
    F.lit(":00"),)

    hour = F.substring(F.col(col_name), 1, 2).cast("int")
    minute = F.substring(F.col(col_name), 3, 2).cast("int")

    is_valid_time = (
        F.col(col_name).rlike(r"^[0-9]{4}$") &
        hour.between(0, 23) &
        minute.between(0, 59)
    )

    return F.when(is_valid_time, F.trim(F.col(col_name))).otherwise(F.lit(None).cast("string"))


def build_home_schedule(df: DataFrame) -> DataFrame:
    """Build the home priority schedule DataFrame ready for HOME_BASIC_JNR (left side).

    Implements HOME_CPY + HOME_SEQ1/2/3_PVT + HOME_SEQ2/3_TFM + HOME_SEQ_FNL + INC_PRTY_TFM.

    Args:
        df: HOME type DataFrame from phone_router (home_df, HOME_TYPE_CPY schema).

    Returns:
        DataFrame with (EMP_NBR, CALL_LIST, CALL_PRTY, LKP_CALL_PRTY,
        TELE_HOME_PRI_FROM, TELE_HOME_PRI_TO, UPD_TS, USER_ID).
        Filtered: isvalid("int32", EMP_NBR) and isvalid("int32", LKP_CALL_PRTY).
    """
    # HOME_CPY + HOME_SEQ1_PVT: group 1, CALL_PRTY 1–5 (no offset)
    seq1_df = _unpivot_seq_group(df, 1)

    # HOME_SEQ2_PVT + HOME_SEQ2_TFM: group 2, CALL_PRTY 1–5 offset +5 → 6–10
    seq2_raw_df = _unpivot_seq_group(df, 2)
    seq2_df = seq2_raw_df.withColumn("CALL_PRTY", F.col("CALL_PRTY") + F.lit(5))

    # HOME_SEQ3_PVT + HOME_SEQ3_TFM: group 3, CALL_PRTY 1–5 offset +10 → 11–15
    seq3_raw_df = _unpivot_seq_group(df, 3)
    seq3_df = seq3_raw_df.withColumn("CALL_PRTY", F.col("CALL_PRTY") + F.lit(10))

    # HOME_SEQ_FNL: union all 3 groups
    home_seq_df = (
        seq1_df
        .unionByName(seq2_df, allowMissingColumns=True)
        .unionByName(seq3_df, allowMissingColumns=True)
    )

    # INC_PRTY_TFM: filter, apply CALL_PRTY+1, validate time strings, assign CALL_LIST='HOME'
    result = (
        home_seq_df
        .filter(
            F.col("EMP_NBR").cast("int").isNotNull()
            & F.col("LKP_CALL_PRTY").cast("int").isNotNull()
        )
        .withColumn("CALL_PRTY", F.col("CALL_PRTY") + F.lit(1))
        .withColumn("CALL_LIST", F.lit("HOME"))
        .withColumn("TELE_HOME_PRI_FROM", _validate_time("TELE_HOME_PRI_FROM"))
        .withColumn("TELE_HOME_PRI_TO", _validate_time("TELE_HOME_PRI_TO"))
    )
    return result
