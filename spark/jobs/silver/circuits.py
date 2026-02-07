from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("/opt/spark/app_lib")

from logging_config import console_log_ingestion
from utils import get_most_recent_data

sys.path.append("/opt/spark/jobs/..")

from jobs.schemas.meetings import json_schema


def main(year: int) -> None:
    silver_table_name = "circuits"

    spark = SparkSession.builder.appName(f"silver_transform_{silver_table_name}").getOrCreate()

    bronze_path = os.environ.get("BRONZE_MEETINGS_DELTA_PATH")

    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .withColumn("year", F.get_json_object("raw", "$.year").cast("int"))
        .filter(F.col("year") == year)
    )

    # Get most recent ingestion timestamp
    most_recent_data = get_most_recent_data(bronze_df, "ingestion_ts")

    bronze_df_with_struct = most_recent_data.withColumn("json", F.from_json("raw", json_schema)).drop("raw")

    run_ts = datetime.now(timezone.utc).isoformat()

    silver_df = (
        bronze_df_with_struct
        .select (
            F.col("json.circuit_key").alias("circuit_key")
            , F.col("json.circuit_short_name").alias("circuit_short_name")
            , F.col("json.circuit_type").alias("circuit_type")
            , F.col("json.circuit_info_url").alias("circuit_info_url")
            , F.col("json.circuit_image").alias("circuit_image")
            , F.col("json.year").alias("year") # partition 
            , F.lit(run_ts).alias("run_ts")
            , F.col("ingestion_ts").cast("timestamp").alias("bronze_ingestion_ts") # easy to check if the data is up to date
            , F.col("request_id").alias("request_id") # FK to bronze layer
        )
        .distinct()
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

    console_log_ingestion(silver_table_name, silver_df, silver_path, request_id)
    
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Transform: Circuits")
    parser.add_argument("--year", required=True, help='Year to filter Meetings Bronze table')
    args = parser.parse_args()

    main(year=args.year)
