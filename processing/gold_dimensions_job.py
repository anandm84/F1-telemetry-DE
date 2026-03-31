import os
import re
from datetime import datetime, timezone

import fastf1
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, row_number
from pyspark.sql.window import Window

PROJECT_ROOT = os.getenv("F1_PROJECT_ROOT", "/opt/project")
SILVER_RESULTS_PATH = f"{PROJECT_ROOT}/data/silver/race_results"
GOLD_DIMS_BASE = f"{PROJECT_ROOT}/data/gold/dims"
GOLD_OUTPUT_FILES = int(os.getenv("GOLD_OUTPUT_FILES", "1"))

fastf1.Cache.enable_cache(f"{PROJECT_ROOT}/cache")

spark = (
    SparkSession.builder
    .appName("F1 Gold Dimensions Batch")
    .getOrCreate()
)

# ── dim_drivers ───────────────────────────────────────────────────────────────
# Built from silver race results — latest team/nationality per driver wins.

results_df = spark.read.parquet(SILVER_RESULTS_PATH)

driver_window = Window.partitionBy("driver_id").orderBy(
    col("race_year").desc(), col("race_round").desc()
)

dim_drivers = (
    results_df
    .filter(col("driver_id").isNotNull() & (col("driver_id") != ""))
    .withColumn("_rn", row_number().over(driver_window))
    .filter(col("_rn") == 1)
    .withColumn("_updated_at", current_timestamp())
    .select("driver_id", "full_name", "team", "nationality", "_updated_at")
    .coalesce(GOLD_OUTPUT_FILES)
)

dim_drivers.write.mode("overwrite").parquet(f"{GOLD_DIMS_BASE}/dim_drivers")
print(f"[gold_dims] dim_drivers written: {dim_drivers.count()} rows")

# ── dim_circuits & dim_races ──────────────────────────────────────────────────
# Built from FastF1 event schedules for each year present in silver results.

years = [row["race_year"] for row in results_df.select("race_year").distinct().collect()]


def _slug(text):
    if not text:
        return "unknown"
    return re.sub(r"[^a-z0-9]+", "_", str(text).strip().lower()).strip("_")


circuits_rows = []
races_rows = []
now_ts = datetime.now(timezone.utc).isoformat()

for year in sorted(years):
    try:
        schedule = fastf1.get_event_schedule(year, include_testing=False)
    except Exception as e:
        print(f"[gold_dims] Could not fetch schedule for {year}: {e}")
        continue

    for _, event in schedule.iterrows():
        round_num = event.get("RoundNumber")
        if round_num is None or round_num != round_num:  # NaN check
            continue
        round_num = int(round_num)

        location = str(event.get("Location", "") or "")
        circuit_id = _slug(location)
        circuit_name = str(event.get("OfficialEventName", "") or "")
        country = str(event.get("Country", "") or "")

        circuits_rows.append({
            "circuit_id": circuit_id,
            "circuit_name": circuit_name,
            "country": country,
            "locality": location,
            "_updated_at": now_ts,
        })

        event_date = event.get("EventDate")
        try:
            race_date = str(event_date.date()) if hasattr(event_date, "date") else str(event_date)
        except Exception:
            race_date = None

        races_rows.append({
            "race_key": f"{year}_{round_num}",
            "race_year": year,
            "race_round": round_num,
            "race_name": str(event.get("EventName", "") or ""),
            "circuit_id": circuit_id,
            "race_date": race_date,
        })

# Deduplicate circuits by circuit_id (same track appears multiple years)
seen_circuits = {}
for row in circuits_rows:
    seen_circuits[row["circuit_id"]] = row  # last year wins (most recent name)

dim_circuits = (
    spark.createDataFrame(list(seen_circuits.values()))
    .coalesce(GOLD_OUTPUT_FILES)
)
dim_circuits.write.mode("overwrite").parquet(f"{GOLD_DIMS_BASE}/dim_circuits")
print(f"[gold_dims] dim_circuits written: {dim_circuits.count()} rows")

dim_races = (
    spark.createDataFrame(races_rows)
    .dropDuplicates(["race_key"])
    .coalesce(GOLD_OUTPUT_FILES)
)
dim_races.write.mode("overwrite").parquet(f"{GOLD_DIMS_BASE}/dim_races")
print(f"[gold_dims] dim_races written: {dim_races.count()} rows")
