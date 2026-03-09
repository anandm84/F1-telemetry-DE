from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("F1 Silver Layer") \
    .getOrCreate()

df = spark.read.json("/opt/project/data/bronze/laps.ndjson")

cleaned = df.select(
    col("Driver").alias("driver"),
    col("LapNumber").alias("lap_number"),
    col("LapTime").alias("lap_time")
)

cleaned.write.mode("overwrite").parquet("/opt/project/data/silver/lap_times")