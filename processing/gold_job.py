from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("F1 Gold Layer") \
    .getOrCreate()

df = spark.read.parquet("/opt/project/data/silver/lap_times")

agg = df.groupBy("driver") \
    .agg(avg("lap_time").alias("avg_lap_time"))

agg.write.mode("overwrite").parquet("/opt/project/data/gold/driver_summary")