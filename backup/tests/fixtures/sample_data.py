"""Synthetic test data fixtures for all pipeline stages."""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def source_schema() -> StructType:
    """Schema for source DataFrame (LNK_OUT_TDC — 71 columns, abbreviated for test use)."""
    fields = [
        StructField("EMP_NBR", StringType(), True),
        StructField("USER_ID", StringType(), True),
        StructField("PH_COMMENTS", StringType(), True),
        StructField("PH_NBR", StringType(), True),
        StructField("TEMP_PH_NBR", StringType(), True),
        StructField("TELE_TEMP_PH_DATE", StringType(), True),
        StructField("TELE_TEMP_PH_TIME", StringType(), True),
        StructField("TELE_LAST_UPDATED_DATE", StringType(), True),
        StructField("TELE_LAST_UPDATED_TIME", StringType(), True),
    ]
    for i in range(1, 6):
        fields += [
            StructField(f"BASIC_PH_NBR_{i}", StringType(), True),
            StructField(f"BASIC_PH_ACCESS_{i}", StringType(), True),
            StructField(f"BASIC_PH_UNLIST_CD_{i}", StringType(), True),
            StructField(f"BASIC_PH_TYPE_{i}", StringType(), True),
            StructField(f"BASIC_PH_HOME_AWAY_CD_{i}", StringType(), True),
            StructField(f"BASIC_PH_COMMENT_{i}", StringType(), True),
            StructField(f"TEMP_PH_EXP_TS_{i}", TimestampType(), True),
        ]
    for g in range(1, 4):
        fields += [
            StructField(f"TELE_HOME_PRI_FROM_{g}", StringType(), True),
            StructField(f"TELE_HOME_PRI_TO_{g}", StringType(), True),
        ]
        for s in range(1, 6):
            fields.append(StructField(f"TELE_HOME_PRI_SEQ_{s}_{g}", StringType(), True))
        fields += [
            StructField(f"TELE_AWAY_PRI_FROM_{g}", StringType(), True),
            StructField(f"TELE_AWAY_PRI_TO_{g}", StringType(), True),
        ]
        for s in range(1, 6):
            fields.append(StructField(f"TELE_AWAY_PRI_SEQ_{s}_{g}", StringType(), True))
    return StructType(fields)


def make_source_row(**overrides) -> dict:
    """Return a minimal valid source row with optional field overrides."""
    defaults = {
        "EMP_NBR": "123456789",
        "USER_ID": "BSMITH",
        "PH_COMMENTS": "Emergency contact",
        "PH_NBR": "2125551234",
        "TEMP_PH_NBR": None,
        "TELE_TEMP_PH_DATE": None,
        "TELE_TEMP_PH_TIME": None,
        "TELE_LAST_UPDATED_DATE": "20240101",
        "TELE_LAST_UPDATED_TIME": "120000",
    }
    for i in range(1, 6):
        defaults[f"BASIC_PH_NBR_{i}"] = f"21255500{i:02d}" if i == 1 else None
        defaults[f"BASIC_PH_ACCESS_{i}"] = None
        defaults[f"BASIC_PH_UNLIST_CD_{i}"] = "N"
        defaults[f"BASIC_PH_TYPE_{i}"] = "W" if i == 1 else None
        defaults[f"BASIC_PH_HOME_AWAY_CD_{i}"] = "H" if i == 1 else None
        defaults[f"BASIC_PH_COMMENT_{i}"] = ""
        defaults[f"TEMP_PH_EXP_TS_{i}"] = None
    for g in range(1, 4):
        defaults[f"TELE_HOME_PRI_FROM_{g}"] = None
        defaults[f"TELE_HOME_PRI_TO_{g}"] = None
        for s in range(1, 6):
            defaults[f"TELE_HOME_PRI_SEQ_{s}_{g}"] = None
        defaults[f"TELE_AWAY_PRI_FROM_{g}"] = None
        defaults[f"TELE_AWAY_PRI_TO_{g}"] = None
        for s in range(1, 6):
            defaults[f"TELE_AWAY_PRI_SEQ_{s}_{g}"] = None
    defaults.update(overrides)
    return defaults
