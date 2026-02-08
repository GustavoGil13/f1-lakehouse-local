import argparse
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("/opt/spark/app_lib")

from logging_config import console_log_ingestion
from utils import get_most_recent_data, setup_metadata_columns, apply_gmt_offset

sys.path.append("/opt/spark/jobs/..")

from jobs.schemas.sessions import json_schema


def main(silver_table_name: str, year: int) -> None:
    spark = SparkSession.builder.appName(f"silver_transform_{silver_table_name}").getOrCreate()

    bronze_path = os.environ.get("BRONZE_DELTA_PATH") + "sessions"

    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .withColumn("year", F.get_json_object("raw", "$.year").cast("int"))
        .filter(F.col("year") == year)
    )

    # Get most recent ingestion timestamp
    most_recent_data = get_most_recent_data(bronze_df, "ingestion_ts")

    bronze_df_with_struct = most_recent_data.withColumn("json", F.from_json("raw", json_schema)).drop("raw")

    _, run_ts = setup_metadata_columns()

    silver_df = (
        bronze_df_with_struct
        .select (
            F.col("json.session_key").alias("session_key")
            , F.col("json.meeting_key").alias("meeting_key")
            , F.col("json.country_key").alias("country_key")
            , F.xxhash64("json.location").alias("location_key")
            , F.col("json.session_name").alias("session_name")
            , F.col("json.session_type").alias("session_type")
            , F.col("json.date_start").alias("local_ts_start") # timestamps
            , F.col("json.date_end").alias("local_ts_end") # timestamps
            , apply_gmt_offset ( # normalize timestamps
                F.col("json.date_start")
                , F.col("json.gmt_offset")
            ).alias("ts_start")
            , apply_gmt_offset ( # normalize timestamps
                F.col("json.date_end")
                , F.col("json.gmt_offset")
            ).alias("ts_end")
            , F.col("json.year").alias("year") # partition 
            , F.lit(run_ts).alias("run_ts")
            , F.col("ingestion_ts").cast("timestamp").alias("bronze_ingestion_ts") # easy to check if the data is up to date
            , F.col("request_id").alias("request_id") # FK to bronze layer
        )
    )

    request_id = silver_df.select("request_id").distinct().first()[0]

    silver_path = os.environ.get("SILVER_DELTA_PATH") + silver_table_name

    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("year")
        .save(silver_path)
    )

    console_log_ingestion(silver_table_name, silver_df.count(), silver_path, request_id)
    
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Transform: Sessions")
    parser.add_argument("--year", required=True, help='Year to filter Sessions Bronze table')
    args = parser.parse_args()

    main("sessions", year=args.year)
