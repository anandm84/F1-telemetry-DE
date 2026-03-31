import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, upper
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

PROJECT_ROOT = os.getenv("F1_PROJECT_ROOT", "/opt/project")
BRONZE_PATH = f"{PROJECT_ROOT}/data/bronze/race_results"
BRONZE_GLOB = "results.ndjson.session-*.driver-*.ndjson"
SILVER_PATH = f"{PROJECT_ROOT}/data/silver/race_results"
SILVER_OUTPUT_FILES = int(os.getenv("SILVER_OUTPUT_FILES", "1"))

spark = (
    SparkSession.builder
    .appName("F1 Silver Race Results Batch")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("record_id", StringType(), True),
        StructField("race_year", IntegerType(), True),
        StructField("race_round", IntegerType(), True),
        StructField("session", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("team", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("position", IntegerType(), True),
        StructField("grid_position", IntegerType(), True),
        StructField("points", DoubleType(), True),
        StructField("status", StringType(), True),
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
    .withColumn("driver_id", upper(trim(col("driver_id"))))
    .filter(col("record_id").isNotNull())
    .filter(col("driver_id").isNotNull() & (col("driver_id") != ""))
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
        "driver_id",
        "full_name",
        "team",
        "nationality",
        "position",
        "grid_position",
        "points",
        "status",
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
    )
    .coalesce(SILVER_OUTPUT_FILES)
)

silver_df.write.mode("overwrite").parquet(SILVER_PATH)
