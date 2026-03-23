"""Phone type router — implements SEPARATE_PHTYPE_TFM (CTransformerStage).

Multi-cast router: one source row can appear in multiple output streams simultaneously.
Routes are NOT mutually exclusive — a single employee row can produce EMERGENCY + BASIC
+ HOME outputs at the same time.
"""

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _ts_from_date_time(date_col: str, time_col: str) -> F.Column:
    """Reconstruct a timestamp from YYYYMMDD date and HHMMSS time string columns.

    Args:
        date_col: Column name containing date in YYYYMMDD format.
        time_col: Column name containing time in HHMMSS format.

    Returns:
        Timestamp column expression.
    """
    return F.to_timestamp(
        F.concat(
            F.substring(F.col(date_col), 1, 4),
            F.lit("-"),
            F.substring(F.col(date_col), 5, 2),
            F.lit("-"),
            F.substring(F.col(date_col), 7, 2),
            F.lit(" "),
            F.substring(F.col(time_col), 1, 2),
            F.lit(":"),
            F.substring(F.col(time_col), 3, 2),
            F.lit(":"),
            F.substring(F.col(time_col), 5, 2),
        ),
        "yyyy-MM-dd HH:mm:ss",
    )


def _upd_ts() -> F.Column:
    """Return UPD_TS column expression (timestamp from TELE_LAST_UPDATED_DATE/TIME)."""
    return _ts_from_date_time("TELE_LAST_UPDATED_DATE", "TELE_LAST_UPDATED_TIME").alias("UPD_TS")


def _null_clean_string(col_name: str) -> F.Column:
    """Return null if string is empty/whitespace, else return trimmed value.

    Maps DataStage: if trim_leading_trailing(X)='' or not(is_valid("string",X)) then set_null()
    else trim_leading_trailing(X).

    Args:
        col_name: Name of the string column to clean.

    Returns:
        Column expression with null-cleaning applied.
    """
    trimmed = F.trim(F.coalesce(F.col(col_name), F.lit("")))
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed).alias(col_name)


from pyspark.sql import functions as F

def route_by_phone_type(df: DataFrame, config: dict[str, Any]) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    valid_emp = F.expr("try_cast(EMP_NBR as int) is not null")
    valid_ph = F.expr("try_cast(PH_NBR as bigint) is not null")
    valid_temp_ph = F.expr("try_cast(TEMP_PH_NBR as bigint) is not null")

    emergency_df = df.filter(
        valid_emp & valid_ph
    ).select(
        F.col("EMP_NBR"),
        F.lit("EMERGENCY").alias("CALL_LIST"),
        F.lit(1).alias("CALL_PRTY"),
        F.lit("0000").alias("EFF_START_TM"),
        F.lit("2359").alias("EFF_END_TM"),
        F.col("PH_NBR"),
        F.lit(None).cast("string").alias("PH_ACCESS"),
        _null_clean_string("PH_COMMENTS"),
        F.lit(None).cast("string").alias("PH_TYPE"),
        F.lit(None).cast("string").alias("UNLISTD_IND"),
        F.lit(None).cast("string").alias("HOME_AWAY_IND"),
        F.lit(None).cast("timestamp").alias("TEMP_PH_EXP_TS"),
        _upd_ts(),
        _null_clean_string("USER_ID"),
    )

    temp_df = df.filter(
        valid_emp & valid_temp_ph
    ).select(
        F.col("EMP_NBR"),
        F.lit("TEMP").alias("CALL_LIST"),
        F.lit(1).alias("CALL_PRTY"),
        F.lit("0001").alias("EFF_START_TM"),
        F.lit("2359").alias("EFF_END_TM"),
        F.col("TEMP_PH_NBR").alias("PH_NBR"),
        F.lit(None).cast("string").alias("PH_ACCESS"),
        F.lit(None).cast("string").alias("PH_COMMENTS"),
        F.lit(None).cast("string").alias("PH_TYPE"),
        F.lit(None).cast("string").alias("UNLISTD_IND"),
        F.lit(None).cast("string").alias("HOME_AWAY_IND"),
        _ts_from_date_time("TELE_TEMP_PH_DATE", "TELE_TEMP_PH_TIME").alias("TEMP_PH_EXP_TS"),
        _upd_ts(),
        _null_clean_string("USER_ID"),
    )

    basic_df = df.filter(valid_emp).select(
        F.col("EMP_NBR"),
        F.lit("BASIC").alias("CALL_LIST"),
        F.lit("0001").alias("EFF_START_TM"),
        F.lit("2359").alias("EFF_END_TM"),
        *[F.col(f"BASIC_PH_NBR_{i}") for i in range(1, 6)],
        *[F.col(f"BASIC_PH_ACCESS_{i}") for i in range(1, 6)],
        *[F.col(f"BASIC_PH_UNLIST_CD_{i}") for i in range(1, 6)],
        *[F.col(f"BASIC_PH_TYPE_{i}") for i in range(1, 6)],
        *[F.col(f"BASIC_PH_HOME_AWAY_CD_{i}") for i in range(1, 6)],
        *[F.col(f"BASIC_PH_COMMENT_{i}") for i in range(1, 6)],
        *[F.lit(None).cast("timestamp").alias(f"TEMP_PH_EXP_TS_{i}") for i in range(1, 6)],
        _upd_ts(),
        _null_clean_string("USER_ID"),
    )

    home_df = df.select(
        F.col("EMP_NBR"),
        *[F.col(f"TELE_HOME_PRI_FROM_{i}") for i in range(1, 4)],
        *[F.col(f"TELE_HOME_PRI_TO_{i}") for i in range(1, 4)],
        *[F.col(f"TELE_HOME_PRI_SEQ_{s}_{g}") for g in range(1, 4) for s in range(1, 6)],
        _upd_ts(),
        _null_clean_string("USER_ID"),
    )

    away_df = df.select(
        F.col("EMP_NBR"),
        *[F.col(f"TELE_AWAY_PRI_FROM_{i}").alias(f"TELE_HOME_PRI_FROM_{i}") for i in range(1, 4)],
        *[F.col(f"TELE_AWAY_PRI_TO_{i}").alias(f"TELE_HOME_PRI_TO_{i}") for i in range(1, 4)],
        *[F.col(f"TELE_AWAY_PRI_SEQ_{s}_{g}").alias(f"TELE_HOME_PRI_SEQ_{s}_{g}") for g in range(1, 4) for s in range(1, 6)],
        _upd_ts(),
        _null_clean_string("USER_ID"),
    )

    error_df = df.filter(~valid_emp)
    return emergency_df, temp_df, basic_df, home_df, away_df, error_df
