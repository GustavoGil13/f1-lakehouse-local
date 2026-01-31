import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check_bronze_sessions").getOrCreate()

OUTPUT_PATH = os.environ.get("BRONZE_SESSIONS_DELTA_PATH")
if not OUTPUT_PATH:
    raise RuntimeError("Environment variable BRONZE_SESSIONS_DELTA_PATH is not set")

df = spark.read.format("delta").load(OUTPUT_PATH)

print("rows =", df.count())

df.groupBy("year", "ingestion_ts").count().orderBy("ingestion_ts").show()

spark.stop()
