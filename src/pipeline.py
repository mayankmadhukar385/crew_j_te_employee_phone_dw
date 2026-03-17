"""Pipeline orchestrator — CREW_J_TE_EMPLOYEE_PHONE_DW.

Executes all stages in the exact topological order defined in Section 10 of the
Databricks code generation prompt. Each stage boundary logs row counts.

Stage order:
  1  WSTELE_LND_TDC          → read_source
  2  SEPARATE_PHTYPE_TFM     → route_by_phone_type  (multi-cast)
  3  BASIC_TYPE_PVT + BASIC_REC_TFM → unpivot_basic_phones + route_basic_records
  4A HOME schedule pipeline  → build_home_schedule
  4B AWAY schedule pipeline  → build_away_schedule  (parallel)
  5A HOME join + priority    → join/assign/agg/adjust (HOME path)
  5B AWAY join + priority    → join/assign/agg/adjust (AWAY path)
  6  ALL_REC_FNL             → union_all_phone_types
  7  NULL_VAL_TFM            → finalize_phone_records
  8  TE_EMPLOYEE_PHONE_DW    → write_target
  9  WSSEN_ERR_SEQ           → write_errors
"""

from pyspark.sql import SparkSession

from src.readers.source_reader import read_source
from src.transformations.away_phone_join import (
    assign_away_adjusted_priority,
    assign_away_scheduled_priority,
    compute_away_max_priority,
    join_away_max_to_basic,
    join_away_schedule_to_basic,
)
from src.transformations.away_priority_pipeline import build_away_schedule
from src.transformations.basic_phone_pipeline import route_basic_records, unpivot_basic_phones
from src.transformations.home_phone_join import (
    assign_home_adjusted_priority,
    assign_home_scheduled_priority,
    compute_home_max_priority,
    join_home_max_to_basic,
    join_home_schedule_to_basic,
)
from src.transformations.home_priority_pipeline import build_home_schedule
from src.transformations.phone_finalizer import finalize_phone_records
from src.transformations.phone_router import route_by_phone_type
from src.transformations.phone_union import union_all_phone_types
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.writers.error_writer import write_errors
from src.writers.target_writer import write_target

logger = get_logger(__name__)


def run_pipeline(spark: SparkSession, config_dir: str = "configs") -> None:
    """Execute the full CREW_J_TE_EMPLOYEE_PHONE_DW pipeline.

    Args:
        spark: Active SparkSession with Delta Lake extensions configured.
        config_dir: Path to the configs/ directory.
    """
    config = load_config(config_dir)
    logger.info("Starting pipeline: %s", config["pipeline"]["job_name"])

    # Phase 1: Source extraction
    try:
        source_df = read_source(spark, config)
        logger.info("[1] WSTELE_LND_TDC: %d rows read", source_df.count())
    except Exception as exc:
        logger.error("[1] WSTELE_LND_TDC source read failed: %s", exc, exc_info=True)
        raise

    # Phase 2: Routing (SEPARATE_PHTYPE_TFM)
    try:
        emergency_df, temp_df, basic_input_df, home_input_df, away_input_df, error_df = (
            route_by_phone_type(source_df, config)
        )
        logger.info(
            "[2] SEPARATE_PHTYPE_TFM: EMERGENCY=%d, TEMP=%d, BASIC=%d, HOME=%d, AWAY=%d, ERROR=%d",
            emergency_df.count(), temp_df.count(), basic_input_df.count(),
            home_input_df.count(), away_input_df.count(), error_df.count(),
        )
    except Exception as exc:
        logger.error("[2] SEPARATE_PHTYPE_TFM routing failed: %s", exc, exc_info=True)
        raise

    # Phase 3: BASIC path — unpivot + fan-out (BASIC_TYPE_PVT + BASIC_REC_TFM)
    try:
        all_basic_df, home_join_right_df, home_agg_right_df, away_join_right_df, away_agg_right_df = (
            route_basic_records(unpivot_basic_phones(basic_input_df))
        )
        logger.info("[3] BASIC pipeline: %d direct records", all_basic_df.count())
    except Exception as exc:
        logger.error("[3] BASIC pipeline failed: %s", exc, exc_info=True)
        raise

    # Phase 4: Schedule pipelines (HOME + AWAY)
    try:
        home_sched_left_df = build_home_schedule(home_input_df)
        logger.info("[4A] HOME schedule: %d schedule rows", home_sched_left_df.count())
        away_sched_left_df = build_away_schedule(away_input_df)
        logger.info("[4B] AWAY schedule: %d schedule rows", away_sched_left_df.count())
    except Exception as exc:
        logger.error("[4] Schedule pipeline failed: %s", exc, exc_info=True)
        raise

    # Phase 5: Join + priority assignment (HOME and AWAY)
    try:
        home_joined_df = join_home_schedule_to_basic(home_sched_left_df, home_join_right_df)
        home_sched_out_df, home_cprty_df = assign_home_scheduled_priority(home_joined_df)
        home_max_df = compute_home_max_priority(home_cprty_df)
        home_basic_max_df = join_home_max_to_basic(home_max_df, home_agg_right_df)
        home_adj_df = assign_home_adjusted_priority(home_basic_max_df)
        logger.info("[5A] HOME priority: sched=%d, adj=%d", home_sched_out_df.count(), home_adj_df.count())

        away_joined_df = join_away_schedule_to_basic(away_sched_left_df, away_join_right_df)
        away_sched_out_df, away_cprty_df = assign_away_scheduled_priority(away_joined_df)
        away_max_df = compute_away_max_priority(away_cprty_df)
        away_basic_max_df = join_away_max_to_basic(away_max_df, away_agg_right_df)
        away_adj_df = assign_away_adjusted_priority(away_basic_max_df)
        logger.info("[5B] AWAY priority: sched=%d, adj=%d", away_sched_out_df.count(), away_adj_df.count())
    except Exception as exc:
        logger.error("[5] Join/priority phase failed: %s", exc, exc_info=True)
        raise

    # Phase 6: Union all 7 streams (ALL_REC_FNL)
    try:
        all_df = union_all_phone_types(
            emergency_df, temp_df, all_basic_df,
            home_sched_out_df, home_adj_df,
            away_sched_out_df, away_adj_df,
        )
        logger.info("[6] ALL_REC_FNL union: %d total phone records", all_df.count())
    except Exception as exc:
        logger.error("[6] ALL_REC_FNL union failed: %s", exc, exc_info=True)
        raise

    # Phase 7: Final transformations (NULL_VAL_TFM)
    try:
        final_df = finalize_phone_records(all_df)
        logger.info("[7] NULL_VAL_TFM: %d rows after filter (valid phone numbers only)", final_df.count())
    except Exception as exc:
        logger.error("[7] NULL_VAL_TFM failed: %s", exc, exc_info=True)
        raise

    # Phase 8: Write primary target (TE_EMPLOYEE_PHONE_DW)
    try:
        write_target(final_df, spark, config)
        logger.info("[8] TE_EMPLOYEE_PHONE_DW: written to %s", config["target"]["full_table"])
    except Exception as exc:
        logger.error("[8] Target write failed: %s", exc, exc_info=True)
        raise

    # Phase 9: Write error records (WSSEN_ERR_SEQ)
    try:
        write_errors(error_df, config)
        logger.info("[9] WSSEN_ERR_SEQ: %d error records written", error_df.count())
    except Exception as exc:
        logger.error("[9] Error write failed: %s", exc, exc_info=True)
        raise

    logger.info("Pipeline complete: %s", config["pipeline"]["job_name"])


if __name__ == "__main__":
    _spark = (
        SparkSession.builder
        .appName("CREW_J_TE_EMPLOYEE_PHONE_DW")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    run_pipeline(_spark)
