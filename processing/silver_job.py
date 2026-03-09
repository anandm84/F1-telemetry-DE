import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, length, lit, regexp_extract, to_timestamp, trim, upper, when
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

BRONZE_PATH = "/opt/project/data/bronze"
BRONZE_GLOB = "laps.ndjson.session-*.driver-*.ndjson"
SILVER_PATH = "/opt/project/data/silver/lap_times"
SILVER_OUTPUT_FILES = int(os.getenv("SILVER_OUTPUT_FILES", "1"))

spark = (
    SparkSession.builder
    .appName("F1 Silver Layer Batch")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("Driver", StringType(), True),
        StructField("LapNumber", DoubleType(), True),
        StructField("LapTime", StringType(), True),
        StructField("Sector1Time", StringType(), True),
        StructField("Sector2Time", StringType(), True),
        StructField("Sector3Time", StringType(), True),
        StructField("Compound", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("race_year", IntegerType(), True),
        StructField("race_round", IntegerType(), True),
        StructField("session", StringType(), True),
        StructField("record_id", StringType(), True),
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


def duration_to_ms(column_name):
    """Convert 'HH:MM:SS:MMMM' (or legacy 'D days HH:MM:SS.ssssss') to milliseconds."""
    modern_match = regexp_extract(
        col(column_name),
        r"^(\d{2}):(\d{2}):(\d{2}):(\d{4})$",
        0,
    )
    modern_hours = regexp_extract(col(column_name), r"^(\d{2}):\d{2}:\d{2}:\d{4}$", 1).cast("double")
    modern_minutes = regexp_extract(col(column_name), r"^\d{2}:(\d{2}):\d{2}:\d{4}$", 1).cast("double")
    modern_seconds = regexp_extract(col(column_name), r"^\d{2}:\d{2}:(\d{2}):\d{4}$", 1).cast("double")
    modern_frac_4 = regexp_extract(col(column_name), r"^\d{2}:\d{2}:\d{2}:(\d{4})$", 1).cast("double")
    modern_ms_value = (((modern_hours * 60 + modern_minutes) * 60 + modern_seconds) * 1000) + (modern_frac_4 / 10.0)

    legacy_match = regexp_extract(
        col(column_name),
        r"^(?:(\d+)\s+days\s+)?\d{2}:\d{2}:\d+(?:\.\d+)?$",
        0,
    )
    days = coalesce(regexp_extract(col(column_name), r"(?:(\d+)\s+days\s+)?", 1).cast("double"), lit(0.0))
    hours = coalesce(regexp_extract(col(column_name), r"(?:\d+\s+days\s+)?(\d{2}):", 1).cast("double"), lit(0.0))
    minutes = coalesce(regexp_extract(col(column_name), r"(?:\d+\s+days\s+)?\d{2}:(\d{2}):", 1).cast("double"), lit(0.0))
    seconds = coalesce(regexp_extract(col(column_name), r"(?:\d+\s+days\s+)?\d{2}:\d{2}:(\d+(?:\.\d+)?)", 1).cast("double"), lit(0.0))
    legacy_ms_value = (((days * 24 + hours) * 60 + minutes) * 60 + seconds) * 1000

    return (
        when(length(modern_match) > 0, modern_ms_value)
        .when(length(legacy_match) > 0, legacy_ms_value)
    )


silver_df = (
    raw_df
    .withColumn("event_timestamp", to_timestamp("event_ts"))
    .withColumn("bronze_written_at", to_timestamp("_bronze_written_at"))
    .withColumn("driver_id", upper(trim(col("Driver"))))
    .withColumn("lap_number", col("LapNumber").cast("int"))
    .withColumn("lap_time_ms", duration_to_ms("LapTime").cast("long"))
    .withColumn("sector1_ms", duration_to_ms("Sector1Time").cast("long"))
    .withColumn("sector2_ms", duration_to_ms("Sector2Time").cast("long"))
    .withColumn("sector3_ms", duration_to_ms("Sector3Time").cast("long"))
    .withColumn("tire_compound", upper(trim(col("Compound"))))
    .withColumn("driver", col("driver_id"))
    .withColumn("lap_time", col("lap_time_ms"))
    .filter(col("record_id").isNotNull())
    .filter(col("event_timestamp").isNotNull())
    .filter(col("driver_id").isNotNull() & (col("driver_id") != ""))
    .filter(col("lap_number").isNotNull())
    .filter(col("lap_time_ms").isNotNull())
    .dropDuplicates(["record_id"])
    .select(
        "record_id",
        "race_year",
        "race_round",
        "session",
        "event_timestamp",
        "bronze_written_at",
        "driver_id",
        "driver",
        "lap_number",
        "lap_time_ms",
        "lap_time",
        "sector1_ms",
        "sector2_ms",
        "sector3_ms",
        "tire_compound",
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
    )
    .coalesce(SILVER_OUTPUT_FILES)
)

silver_df.write.mode("overwrite").parquet(SILVER_PATH)
