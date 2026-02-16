import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("/opt/spark/app_lib")

from logging_config import console_log_ingestion
from utils import get_most_recent_data, setup_metadata_columns, setup_db_location

sys.path.append("/opt/spark/jobs/..")

from jobs.schemas.session_result import json_schema


def main(silver_table_name: str, year: int) -> None:
    spark = SparkSession.builder.appName(f"silver_transform_{silver_table_name}").enableHiveSupport().getOrCreate()

    bronze_db, _ = setup_db_location("bronze")

    bronze_df = (
        spark.table(f"{bronze_db}.session_result")
        .filter(F.col("year") == year)
    )

    # Get most recent ingestion timestamp
    most_recent_data = get_most_recent_data(bronze_df, "ingestion_ts")

    bronze_df_with_struct = most_recent_data.withColumn("json", F.from_json("raw", json_schema)).drop("raw")

    _, run_ts = setup_metadata_columns()

    silver_df = (
        bronze_df_with_struct
        .select (
            F.xxhash64 (
                F.concat_ws (
                    "||"
                    , F.col("json.meeting_key")
                    , F.col("json.session_key")
                    , F.col("json.driver_number")
                )
            ).alias("session_result_key")
            , F.col("json.meeting_key").alias("meeting_key")
            , F.col("json.session_key").alias("session_key")
            , F.xxhash64 (
                F.concat_ws (
                    "||"
                    , F.col("json.meeting_key")
                    , F.col("json.session_key")
                    , F.col("json.driver_number")
                )
            ).alias("driver_key")
            , F.col("json.dnf").alias("dnf")
            , F.col("json.dns").alias("dns")
            , F.col("json.dsq").alias("dsq")
            , F.col("json.duration").alias("duration")
            , F.col("json.gap_to_leader").alias("gap_to_leader")
            , F.col("json.number_of_laps").alias("number_of_laps")
            , F.col("json.points").alias("points")
            , F.col("json.position").alias("position")
            , F.col("year").alias("year") # partition 
            , F.lit(run_ts).alias("run_ts")
            , F.col("ingestion_ts").cast("timestamp").alias("bronze_ingestion_ts") # easy to check if the data is up to date
            , F.col("request_id").alias("request_id") # FK to bronze layer
        )
    )

    request_id = silver_df.select("request_id").distinct().first()[0]

    # create database if not exists with location (idempotent)
    silver_db, silver_db_location = setup_db_location("silver")

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
    parser = argparse.ArgumentParser(description="Silver Transform: Session Result")
    parser.add_argument("--year", required=True, help='Year to filter Session Result Bronze table')
    args = parser.parse_args()

    main("session_result", year=args.year)
