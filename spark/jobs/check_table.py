import argparse
import sys
from pyspark.sql import SparkSession

sys.path.append("/opt/spark/app_lib")

from logging_config import console_log_check
from utils import setup_db_location

def main(table_name: str, db_name: str) -> None:
    spark = SparkSession.builder.appName(f"check_{db_name}_{table_name}").enableHiveSupport().getOrCreate()

    db, db_location = setup_db_location(db_name)

    df = spark.table(f"{db}.{table_name}")

    spark.sql(f'SHOW TABLES IN {db}').show(truncate=False)

    # Isto pode ler o Delta log em S3
    spark.sql(f'DESCRIBE EXTENDED {db}.{table_name}').show(200, truncate=False)

    if db == "bronze":
        df.groupBy("ingestion_ts").count().orderBy("ingestion_ts").show(10, False)
    elif db == "silver":
        df.groupBy("year", "run_ts").count().orderBy("run_ts").show(10, False)

    console_log_check(table_name, f"{db_location}/{table_name}", df)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Delta table by name")
    parser.add_argument("--table_name", required=True, help='Table name, e.g. "sessions"')
    parser.add_argument("--db_name", required=True, help='Database name, e.g. "bronze"')
    args = parser.parse_args()

    main(table_name=args.table_name, db_name=args.db_name)
