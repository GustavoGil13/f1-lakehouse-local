from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("test_s3a_write").getOrCreate()

df = spark.range(0, 10).withColumn("source", lit("spark"))
df.write.mode("overwrite").parquet("s3a://lakehouse/tmp/test_parquet_write")

print("OK: wrote parquet to s3a://lakehouse/tmp/test_parquet_write")
spark.stop()
