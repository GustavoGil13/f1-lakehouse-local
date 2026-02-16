import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append("/opt/spark/app_lib")

from logging_config import console_log_ingestion
from utils import get_most_recent_data, setup_metadata_columns, setup_db_location, create_db_if_not_exists

sys.path.append("/opt/spark/jobs/..")

from jobs.schemas.drivers import json_schema


def main(silver_table_name: str, year: int) -> None:
    spark = SparkSession.builder.appName(f"silver_transform_{silver_table_name}").enableHiveSupport().getOrCreate()

    bronze_db, _ = setup_db_location("bronze")

    bronze_df = (
        spark.table(f"{bronze_db}.drivers")
        .filter(F.col("year") == year)
    )

    # Get most recent ingestion timestamp
    most_recent_data = get_most_recent_data(bronze_df, "ingestion_ts")

    bronze_df_with_struct = most_recent_data.withColumn("json", F.from_json("raw", json_schema)).drop("raw")

    _, run_ts = setup_metadata_columns()

    base_df = (
        bronze_df_with_struct
        .filter(F.col("json.team_name").isNotNull())
        .select (
            F.xxhash64(F.col("json.driver_number")).alias("driver_key")
            , F.xxhash64("json.team_name").alias("team_key")
            , F.col("json.driver_number").alias("driver_number")
            , F.col("json.first_name").alias("first_name")
            , F.col("json.last_name").alias("last_name")
            , F.col("json.name_acronym").alias("name_acronym")
            , F.col("json.broadcast_name").alias("broadcast_name")
            , F.col("json.headshot_url").alias("headshot_url")
            , F.col("year").alias("year") # partition 
            , F.lit(run_ts).alias("run_ts")
            , F.col("ingestion_ts").cast("timestamp").alias("bronze_ingestion_ts") # easy to check if the data is up to date
            , F.col("request_id").alias("request_id") # FK to bronze layer
        )
    )

    # There are duplicates for example Lando Norris has both NOR and FOR as name_acronyms
    # We want to keep the rows where the name_acronym is most frequent
    w_freq = Window.partitionBy("driver_key", "name_acronym")
    df_with_freq = base_df.withColumn("acronym_freq", F.count("*").over(w_freq))
    w_pick = Window.partitionBy("driver_key").orderBy(F.col("acronym_freq").desc())

    silver_df = (
        df_with_freq
        .withColumn("rn", F.row_number().over(w_pick))
        .filter(F.col("rn") == 1)
        .drop("rn", "acronym_freq")
    )

    request_id = silver_df.select("request_id").distinct().first()[0]

    # create database if not exists with location (idempotent)
    silver_db, silver_db_location = setup_db_location("silver")

    create_db_if_not_exists(spark, silver_db, silver_db_location)

    silver_path = f"{silver_db_location}/{silver_table_name}"

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("year")
        .save(silver_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_db}.{silver_table_name}
        USING DELTA
        LOCATION '{silver_path}'
    """)

    console_log_ingestion(silver_table_name, silver_df.count(), silver_path, request_id)
    
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Transform: Drivers")
    parser.add_argument("--year", required=True, help='Year to filter Drivers Bronze table')
    args = parser.parse_args()

    main("drivers", year=args.year)
