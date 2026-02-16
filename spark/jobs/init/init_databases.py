import sys
from pyspark.sql import SparkSession

sys.path.append("/opt/spark/app_lib")

from utils import setup_db_location, create_db_if_not_exists

spark = (
    SparkSession.builder
    .appName("hive_db_init")
    .enableHiveSupport()
    .getOrCreate()
)

warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "<not set>")
metastore_uris = spark.sparkContext._jsc.hadoopConfiguration().get("hive.metastore.uris")

print("=== Spark/Hive config (effective) ===")
print(f"spark.sql.warehouse.dir = {warehouse_dir}")
print(f"hive.metastore.uris = {metastore_uris}")
print("=====================================")

dbs = ["bronze", "silver", "dq"]

for env_var in dbs:
    db, location = setup_db_location(env_var)
    print(f"Creating/ensuring DB '{db}' at location '{location}' ...")

    create_db_if_not_exists(spark, db, location)

    print(f"Ensured DB {db} at {location}")

spark.stop()
