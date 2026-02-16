import sys
from pyspark.sql import SparkSession

sys.path.append("/opt/spark/app_lib")

from utils import setup_db_location, create_db_if_not_exists

spark = SparkSession.builder.appName('hive_db_init').enableHiveSupport().getOrCreate()

dbs = [
    "bronze"
    , "silver"
    , "dq"
]

for env_var in dbs:
    db, location = setup_db_location(env_var)

    create_db_if_not_exists(spark, db, location)
    
    print(f"Ensured DB {db} at {location}")

spark.stop()
