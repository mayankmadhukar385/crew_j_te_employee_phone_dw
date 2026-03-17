"""Target writer — implements TE_EMPLOYEE_PHONE_DW (TeradataConnectorPX → Delta Lake).

Two-step Teradata load logic is replaced with a single Delta overwrite.
Equivalent to: TRUNCATE work_table + INSERT, then DELETE final + INSERT SELECT from work.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import get_logger

logger = get_logger(__name__)


def write_target(df: DataFrame, spark: SparkSession, config: dict[str, Any]) -> None:
    """Write the final phone records to the Delta target table (TE_EMPLOYEE_PHONE_DW).

    Implements full-refresh (overwrite) mode. Equivalent DataStage load:
      - TableAction=3: Truncate work table, then insert
      - After-SQL: DELETE FROM final; INSERT INTO final SELECT FROM work

    Args:
        df: Finalised DataFrame from finalize_phone_records (14 columns).
        spark: Active SparkSession.
        config: Pipeline configuration dictionary.
    """
    full_table = config["target"]["full_table"]
    zorder_cols = config["target"].get("zorder_columns", [])

    logger.info("Writing target table: %s", full_table)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )
    logger.info("Target write complete: %s", full_table)

    if config.get("delta", {}).get("optimize_after_write", False) and zorder_cols:
        zorder_str = ", ".join(zorder_cols)
        logger.info("Running OPTIMIZE ZORDER BY (%s) on %s", zorder_str, full_table)
        spark.sql(f"OPTIMIZE {full_table} ZORDER BY ({zorder_str})")
