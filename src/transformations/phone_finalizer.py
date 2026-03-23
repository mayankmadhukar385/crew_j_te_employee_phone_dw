"""Phone record finalizer — implements NULL_VAL_TFM (CTransformerStage).

Applies the final transformations before target write:
  - Filter: only records with a valid int64 PH_NBR
  - EMP_NO: zero-pad EMP_NBR to 9 characters
  - EFF_START_TM / EFF_END_TM: reformat HHMM → HH:MM:00
  - PH_ACCESS / PH_COMMENTS: null-clean (empty/whitespace → NULL)
  - TD_LD_TS: add ETL load timestamp
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


def finalize_phone_records(df: DataFrame) -> DataFrame:
    """Apply NULL_VAL_TFM transformations and filter to the final phone record set.

    Derivation logic verbatim from DataStage XML:
      EMP_NO:       If Len(TrimLeadingTrailing(EMP_NBR))=9 Then ... Else Str('0',9-Len(...)):...
      EFF_START_TM: left(EFF_START_TM,2):':':right(EFF_START_TM,2):':00'
      EFF_END_TM:   left(EFF_END_TM,2):':':right(EFF_END_TM,2):':00'
      PH_ACCESS:    if trim(coalesce(X,''))='' or < ' ' then null else trim(coalesce(X,''))
      PH_COMMENTS:  same null-clean pattern
      TD_LD_TS:     CurrentTimestamp()

    Args:
        df: Unified DataFrame from union_all_phone_types (ALL_REC_OUT_FNL schema).

    Returns:
        Final DataFrame with 14 target columns, ready for target_writer.
    """

    def _null_clean(col_name: str) -> F.Column:
        """Null-clean a string: empty or below space → NULL, else trimmed value."""
        val = F.trim(F.coalesce(F.col(col_name), F.lit("")))
        return (
            F.when((val == "") | (val < F.lit(" ")), F.lit(None).cast("string"))
            .otherwise(val)
            .alias(col_name)
        )

    result = (
        df
        # Filter: isvalid("int64", PH_NBR)
        .filter(F.expr("try_cast(PH_NBR as bigint) is not null"))
        # EMP_NO: zero-pad to 9 chars
        .withColumn(
            "EMP_NO",
            F.lpad(F.trim(F.col("EMP_NBR")), 9, "0"),
        )
        # EFF_START_TM: HHMM → HH:MM:00
        .withColumn(
            "EFF_START_TM",
            F.concat(
                F.substring(F.col("EFF_START_TM"), 1, 2),
                F.lit(":"),
                F.substring(F.col("EFF_START_TM"), 3, 2),
                F.lit(":00"),
            ),
        )
        # EFF_END_TM: HHMM → HH:MM:00
        .withColumn(
            "EFF_END_TM",
            F.concat(
                F.substring(F.col("EFF_END_TM"), 1, 2),
                F.lit(":"),
                F.substring(F.col("EFF_END_TM"), 3, 2),
                F.lit(":00"),
            ),
        )
        # PH_ACCESS null-clean
        .withColumn("PH_ACCESS", _null_clean("PH_ACCESS"))
        # PH_COMMENTS null-clean
        .withColumn("PH_COMMENTS", _null_clean("PH_COMMENTS"))
        # TD_LD_TS: ETL load timestamp
        .withColumn("TD_LD_TS", F.current_timestamp())
        # Project final 14 target columns in order
        .select(
            "EMP_NBR",
            "EMP_NO",
            "PH_LIST",
            "PH_PRTY",
            "EFF_START_TM",
            "EFF_END_TM",
            "PH_NBR",
            "PH_ACCESS",
            "PH_COMMENTS",
            "PH_TYPE",
            "UNLISTD_IND",
            "HOME_AWAY_IND",
            "TEMP_PH_EXP_TS",
            "TD_LD_TS",
        )
    )
    return result
