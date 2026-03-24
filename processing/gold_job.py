import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, min, stddev

SILVER_PATH = "/opt/project/data/silver/lap_times"
GOLD_BASE_PATH = "/opt/project/data/gold"
GOLD_OUTPUT_FILES = int(os.getenv("GOLD_OUTPUT_FILES", "1"))

spark = (
    SparkSession.builder
    .appName("F1 Gold Layer Batch")
    .getOrCreate()
)

silver_df = spark.read.parquet(SILVER_PATH)

driver_pace_df = (
    silver_df.groupBy("race_year", "race_round", "session", "driver")
    .agg(
        avg("lap_time_ms").cast("double").alias("avg_lap_time_ms"),
        min("lap_time_ms").cast("long").alias("fastest_lap_ms"),
        stddev("lap_time_ms").cast("double").alias("lap_std_dev_ms"),
        count("*").cast("long").alias("laps_completed"),
    )
    .coalesce(GOLD_OUTPUT_FILES)
)

tire_performance_df = (
    silver_df.groupBy("race_year", "race_round", "session", "tire_compound")
    .agg(
        avg("lap_time_ms").cast("double").alias("avg_lap_time_ms"),
        count("*").cast("long").alias("laps_run"),
    )
    .coalesce(GOLD_OUTPUT_FILES)
)

sector_analysis_df = (
    silver_df.groupBy("race_year", "race_round", "session", "driver")
    .agg(
        avg("sector1_ms").cast("double").alias("avg_sector1_ms"),
        avg("sector2_ms").cast("double").alias("avg_sector2_ms"),
        avg("sector3_ms").cast("double").alias("avg_sector3_ms"),
    )
    .coalesce(GOLD_OUTPUT_FILES)
)

driver_pace_df.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}/driver_pace")
tire_performance_df.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}/tire_performance")
sector_analysis_df.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}/sector_analysis")
