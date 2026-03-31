import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, upper
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

PROJECT_ROOT = os.getenv("F1_PROJECT_ROOT", "/opt/project")
BRONZE_PATH = f"{PROJECT_ROOT}/data/bronze/laps"
BRONZE_GLOB = "laps.ndjson.session-*.driver-*.ndjson"
SILVER_PATH = f"{PROJECT_ROOT}/data/silver/pit_stops"
SILVER_OUTPUT_FILES = int(os.getenv("SILVER_OUTPUT_FILES", "1"))

spark = (
    SparkSession.builder
    .appName("F1 Silver Pit Stops Batch")
    .getOrCreate()
)

# Read only the fields needed — same laps bronze files as silver_job.py
schema = StructType(
    [
        StructField("record_id", StringType(), True),
        StructField("Driver", StringType(), True),
        StructField("LapNumber", DoubleType(), True),
        StructField("Compound", StringType(), True),
        StructField("PitInTime_ms", LongType(), True),
        StructField("PitOutTime_ms", LongType(), True),
        StructField("event_ts", StringType(), True),
        StructField("race_year", IntegerType(), True),
        StructField("race_round", IntegerType(), True),
        StructField("session", StringType(), True),
        StructField("_kafka_topic", StringType(), True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_kafka_timestamp", LongType(), True),
        StructField("_bronze_written_at", StringType(), True),
    ]
)

raw_df = (
    spark.read
    .schema(schema)
    .json(f"{BRONZE_PATH}/{BRONZE_GLOB}")
)

pit_stops_df = (
    raw_df
    # Only rows where the driver actually pitted on this lap
    .filter(col("PitInTime_ms").isNotNull())
    .withColumn("event_timestamp", to_timestamp("event_ts"))
    .withColumn("bronze_written_at", to_timestamp("_bronze_written_at"))
    .withColumn("driver_id", upper(trim(col("Driver"))))
    .withColumn("lap_number", col("LapNumber").cast("int"))
    .withColumn("tire_compound_before", upper(trim(col("Compound"))))
    .filter(col("record_id").isNotNull())
    .filter(col("driver_id").isNotNull() & (col("driver_id") != ""))
    .filter(col("lap_number").isNotNull())
    .dropDuplicates(["record_id"])
    .select(
        "record_id",
        "race_year",
        "race_round",
        "session",
        "event_timestamp",
        "bronze_written_at",
        "driver_id",
        "lap_number",
        col("PitInTime_ms").alias("pit_in_ms"),
        col("PitOutTime_ms").alias("pit_out_ms"),
        "tire_compound_before",
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
    )
    .coalesce(SILVER_OUTPUT_FILES)
)

pit_stops_df.write.mode("overwrite").parquet(SILVER_PATH)
