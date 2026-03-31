import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType

PROJECT_ROOT = os.getenv("F1_PROJECT_ROOT", "/opt/project")
BRONZE_PATH = f"{PROJECT_ROOT}/data/bronze/weather"
BRONZE_GLOB = "weather.ndjson.session-*.ndjson"
SILVER_PATH = f"{PROJECT_ROOT}/data/silver/weather"
SILVER_OUTPUT_FILES = int(os.getenv("SILVER_OUTPUT_FILES", "1"))

spark = (
    SparkSession.builder
    .appName("F1 Silver Weather Batch")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("record_id", StringType(), True),
        StructField("race_year", IntegerType(), True),
        StructField("race_round", IntegerType(), True),
        StructField("session", StringType(), True),
        StructField("snapshot_index", IntegerType(), True),
        StructField("time_offset_ms", LongType(), True),
        StructField("air_temp_c", DoubleType(), True),
        StructField("track_temp_c", DoubleType(), True),
        StructField("humidity_pct", DoubleType(), True),
        StructField("pressure_mbar", DoubleType(), True),
        StructField("wind_speed_ms", DoubleType(), True),
        StructField("is_raining", BooleanType(), True),
        StructField("event_ts", StringType(), True),
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

silver_df = (
    raw_df
    .withColumn("event_timestamp", to_timestamp("event_ts"))
    .withColumn("bronze_written_at", to_timestamp("_bronze_written_at"))
    .filter(col("record_id").isNotNull())
    .filter(col("race_year").isNotNull())
    .filter(col("race_round").isNotNull())
    .dropDuplicates(["record_id"])
    .select(
        "record_id",
        "race_year",
        "race_round",
        "session",
        "event_timestamp",
        "bronze_written_at",
        "snapshot_index",
        "time_offset_ms",
        "air_temp_c",
        "track_temp_c",
        "humidity_pct",
        "pressure_mbar",
        "wind_speed_ms",
        "is_raining",
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
    )
    .coalesce(SILVER_OUTPUT_FILES)
)

silver_df.write.mode("overwrite").parquet(SILVER_PATH)
