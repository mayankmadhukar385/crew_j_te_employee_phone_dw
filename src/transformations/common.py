"""Shared utilities for transformation modules."""

from pyspark.sql import functions as F


def null_clean_string(col_name: str) -> F.Column:
    """Return null if string is empty/whitespace, else return trimmed value.

    Args:
        col_name: Name of the column to clean.

    Returns:
        Column expression.
    """
    trimmed = F.trim(F.coalesce(F.col(col_name), F.lit("")))
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed).alias(col_name)


def ts_from_date_time(date_col: str, time_col: str) -> F.Column:
    """Reconstruct a timestamp from YYYYMMDD + HHMMSS string columns.

    Args:
        date_col: Column name with YYYYMMDD date string.
        time_col: Column name with HHMMSS time string.

    Returns:
        Timestamp column expression.
    """
    return F.to_timestamp(
        F.concat(
            F.substring(F.col(date_col), 1, 4), F.lit("-"),
            F.substring(F.col(date_col), 5, 2), F.lit("-"),
            F.substring(F.col(date_col), 7, 2), F.lit(" "),
            F.substring(F.col(time_col), 1, 2), F.lit(":"),
            F.substring(F.col(time_col), 3, 2), F.lit(":"),
            F.substring(F.col(time_col), 5, 2),
        ),
        "yyyy-MM-dd HH:mm:ss",
    )
