"""Generic Delta Lake writer utilities."""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable

from src.utils.logger import get_logger

logger = get_logger(__name__)


def write_delta(
    df: DataFrame,
    spark: SparkSession,
    config: dict[str, Any],
    full_table: str,
    mode: str = "overwrite",
) -> None:
    """Write a DataFrame to a Delta table with optional OPTIMIZE/ZORDER.

    Args:
        df: DataFrame to write.
        spark: Active SparkSession.
        config: Pipeline configuration dictionary.
        full_table: Fully-qualified table name (catalog.schema.table).
        mode: Write mode — 'overwrite' or 'merge'.
    """
    if mode == "overwrite":
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)
    elif mode == "merge":
        merge_keys = config["target"].get("key_columns", [])
        condition = " AND ".join([f"tgt.{k} = src.{k}" for k in merge_keys])
        (
            DeltaTable.forName(spark, full_table).alias("tgt")
            .merge(df.alias("src"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        raise ValueError(f"Unsupported write mode: {mode}")
