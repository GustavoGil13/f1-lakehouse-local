import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("/opt/spark/app_lib")

from logging_config import console_log_ingestion
from utils import get_most_recent_data, setup_metadata_columns, setup_db_location, map_sector_array

sys.path.append("/opt/spark/jobs/..")

from jobs.schemas.laps import json_schema, sector_map


def main(silver_table_name: str, year: int) -> None:
    spark = SparkSession.builder.appName(f"silver_transform_{silver_table_name}").enableHiveSupport().getOrCreate()
    # Only overwrite the partitions that are being processed, not the entire table
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    bronze_db, _ = setup_db_location("bronze")

    bronze_df = (
        spark.table(f"{bronze_db}.laps")
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
                    , F.col("json.lap_number")
                    , F.col("json.driver_number")
                )
            ).alias("laps_key")
            , F.col("json.meeting_key").alias("meeting_key")
            , F.col("json.session_key").alias("session_key")
            , F.col("json.lap_number").alias("lap_number")
            , F.col("json.driver_number").alias("driver_number")
            , F.col("json.date_start").alias("ts_start")
            , F.col("json.duration_sector_1").alias("duration_sector_1")
            , F.col("json.duration_sector_2").alias("duration_sector_2")
            , F.col("json.duration_sector_3").alias("duration_sector_3")
            , F.col("json.i1_speed").alias("first_intermediate_point_speed")
            , F.col("json.i2_speed").alias("second_intermediate_point_speed")
            , F.col("json.is_pit_out_lap").alias("is_pit_out_lap")
            , F.col("json.lap_duration").alias("lap_duration")
            , F.col("json.segments_sector_1").alias("segments_sector_1_codes")
            , map_sector_array(F.col("json.segments_sector_1"), sector_map).alias("segments_sector_1_colors")
            , F.col("json.segments_sector_2").alias("segments_sector_2_codes")
            , map_sector_array(F.col("json.segments_sector_2"), sector_map).alias("segments_sector_2_colors")
            , F.col("json.segments_sector_3").alias("segments_sector_3_codes")
            , map_sector_array(F.col("json.segments_sector_3"), sector_map).alias("segments_sector_3_colors")
            , F.col("json.st_speed").alias("speed_trap_speed")
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
    parser = argparse.ArgumentParser(description="Silver Transform: Laps")
    parser.add_argument("--year", required=True, help='Year to filter Laps Bronze table')
    args = parser.parse_args()

    main("laps", year=args.year)
