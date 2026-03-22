# Databricks notebook: run_pipeline.py
# Run CREW_J_TE_EMPLOYEE_PHONE_DW pipeline
# Attach to a cluster with Delta Lake + JDBC driver for Teradata

import sys
sys.path.insert(0, "/Workspace/Repos/crew_j_te_employee_phone_dw")

from pyspark.sql import SparkSession
from src.pipeline import run_pipeline

spark = SparkSession.builder.getOrCreate()

# Override config via environment or dbutils.widgets if needed:
# dbutils.widgets.text("UNITY_CATALOG", "my_catalog")
# dbutils.widgets.text("UNITY_SCHEMA", "crew_dw")
# dbutils.widgets.text("TD_WORK_DB", "CREW_WORK")

run_pipeline(spark, config_dir="/Workspace/Repos/crew_j_te_employee_phone_dw/configs")
