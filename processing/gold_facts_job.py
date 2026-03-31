import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit

PROJECT_ROOT = os.getenv("F1_PROJECT_ROOT", "/opt/project")
SILVER_RESULTS_PATH = f"{PROJECT_ROOT}/data/silver/race_results"
SILVER_PIT_STOPS_PATH = f"{PROJECT_ROOT}/data/silver/pit_stops"
SILVER_WEATHER_PATH = f"{PROJECT_ROOT}/data/silver/weather"
GOLD_FACTS_BASE = f"{PROJECT_ROOT}/data/gold/facts"
GOLD_OUTPUT_FILES = int(os.getenv("GOLD_OUTPUT_FILES", "1"))

spark = (
    SparkSession.builder
    .appName("F1 Gold Facts Batch")
    .getOrCreate()
)


def _race_key(df):
    return df.withColumn(
        "race_key",
        concat(col("race_year").cast("string"), lit("_"), col("race_round").cast("string")),
    )


# ── fact_race_results ─────────────────────────────────────────────────────────

results_df = spark.read.parquet(SILVER_RESULTS_PATH)

fact_race_results = (
    _race_key(results_df)
    .select(
        "record_id",
        "race_year",
        "race_round",
        "session",
        "race_key",
        "driver_id",
        "full_name",
        "team",
        "position",
        "grid_position",
        "points",
        "status",
    )
    .coalesce(GOLD_OUTPUT_FILES)
)

fact_race_results.write.mode("overwrite").parquet(f"{GOLD_FACTS_BASE}/fact_race_results")
print(f"[gold_facts] fact_race_results written: {fact_race_results.count()} rows")

# ── fact_pit_stops ────────────────────────────────────────────────────────────

pit_stops_df = spark.read.parquet(SILVER_PIT_STOPS_PATH)

fact_pit_stops = (
    _race_key(pit_stops_df)
    .select(
        "record_id",
        "race_year",
        "race_round",
        "session",
        "race_key",
        "driver_id",
        "lap_number",
        "pit_in_ms",
        "pit_out_ms",
        "tire_compound_before",
    )
    .coalesce(GOLD_OUTPUT_FILES)
)

fact_pit_stops.write.mode("overwrite").parquet(f"{GOLD_FACTS_BASE}/fact_pit_stops")
print(f"[gold_facts] fact_pit_stops written: {fact_pit_stops.count()} rows")

# ── fact_weather_snapshots ────────────────────────────────────────────────────

weather_df = spark.read.parquet(SILVER_WEATHER_PATH)

fact_weather_snapshots = (
    _race_key(weather_df)
    .select(
        "record_id",
        "race_year",
        "race_round",
        "session",
        "race_key",
        "snapshot_index",
        "time_offset_ms",
        "air_temp_c",
        "track_temp_c",
        "humidity_pct",
        "pressure_mbar",
        "wind_speed_ms",
        "is_raining",
    )
    .coalesce(GOLD_OUTPUT_FILES)
)

fact_weather_snapshots.write.mode("overwrite").parquet(f"{GOLD_FACTS_BASE}/fact_weather_snapshots")
print(f"[gold_facts] fact_weather_snapshots written: {fact_weather_snapshots.count()} rows")
