"""Error writer — implements WSSEN_ERR_SEQ (PxSequentialFile → Delta Lake).

Records where EMP_NBR is not a valid int32 are written to a Delta error table.
All 71 source columns are passed through without transformation.
"""

from typing import Any

from pyspark.sql import DataFrame

from src.utils.logger import get_logger

logger = get_logger(__name__)


def write_errors(df: DataFrame, config: dict[str, Any]) -> None:
    """Write error records (invalid EMP_NBR) to the Delta error table (WSSEN_ERR_SEQ).

    Args:
        df: Error DataFrame — all 71 source columns passthrough; no transformation.
        config: Pipeline configuration dictionary.
    """
    if not config.get("error", {}).get("enabled", True):
        logger.info("Error writing disabled by config; skipping %d error records", df.count())
        return

    error_table = config["error"]["table"]
    logger.info("Writing error records to: %s", error_table)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(error_table)
    )
    logger.info("Error write complete: %s", error_table)
