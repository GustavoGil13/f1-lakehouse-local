import argparse
import sys
from typing import Callable, Dict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Make sure spark/lib is importable
sys.path.append("/opt/spark/app_lib")

from openf1_client import fetch_json
from utils import json_serialize, get_most_recent_data, setup_metadata_columns, setup_db_location, create_db_if_not_exists
from logging_config import console_log_key, console_log_ingestion

sys.path.append("/opt/spark/jobs/..")

from jobs.schemas.ingestion_by_key import json_schema

OPERATORS: Dict[str, Callable] = {
    "eq": lambda c, v: c == v
    , "ne": lambda c, v: c != v
}

def main (
        source_endpoint: str
        , year: int
        , key: str
        , target_endpoint: str
        , target_endpoint_filter_column=None
        , target_endpoint_filter_operator="eq"
        , target_endpoint_filter_value=None
    ) -> None:

    spark = SparkSession.builder.appName(f"bronze_ingestion_by_key_{source_endpoint}_{key}_{target_endpoint}_{year}").enableHiveSupport().getOrCreate()

    bronze_db, bronze_db_location = setup_db_location("bronze")

    # Read bronze table filtered by year
    session_filtered_by_year = (
            spark.table(f"{bronze_db}.{source_endpoint}")
            .withColumn("year", F.get_json_object("raw", "$.year").cast("int"))
            .withColumn(key, F.get_json_object("raw", f"$.{key}").cast("int"))
            .filter(
                (
                    (F.col("year") == year)
                    & (OPERATORS[target_endpoint_filter_operator](F.get_json_object("raw", f"$.{target_endpoint_filter_column}").cast("string"), target_endpoint_filter_value) if target_endpoint_filter_column is not None else F.lit(True))
                )
            )
            .select(
                "ingestion_ts"
                , "year"
                , key
            )
    )

    # Get most recent ingestion timestamp
    most_recent_data, max_ingestion_ts = get_most_recent_data(session_filtered_by_year, "ingestion_ts", True)

    # Get keys from most recent ingestion timestamp
    keys = most_recent_data.select(key).distinct().collect()
    keys_list = [row[key] for row in keys]
    console_log_key(key, keys_list)

    # Fetch endpoint data per key
    all_raw = []
    request_id, ingestion_ts = setup_metadata_columns()

    for k in keys_list:
        url, params_used, http_status, payload = fetch_json(target_endpoint, params={key: k})
        
        for item in payload:
            all_raw.append (
                {
                    "raw": json_serialize(item)
                    , "ingestion_ts": ingestion_ts
                    , "request_id": request_id
                    , "source_url": url
                    , "request_params": json_serialize(params_used)
                    , "http_status": http_status
                    , "year": year
                    , "source_endpoint_ingestion_ts": max_ingestion_ts
                }
            )
    
    df = spark.createDataFrame(all_raw, schema=json_schema)

    # create database if not exists with location (idempotent)
    create_db_if_not_exists(spark, bronze_db, bronze_db_location)
    
    output_path = f"{bronze_db_location}/{target_endpoint}"

    # Register table in Hive metastore (idempotent)
    (
        df.write.format("delta")
        .mode("append")
        .option("path", output_path)
        .saveAsTable(f"{bronze_db}.{target_endpoint}")
    )

    console_log_ingestion(target_endpoint, df.count(), output_path, request_id)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Ingestion Information by Key")
    parser.add_argument("--source_endpoint", required=True, type=str, help='Source Endpoint to retrieve list of keys')
    parser.add_argument("--year", required=True, type=int, help='Year to filter Source Endpoint')
    parser.add_argument("--key", required=True, type=str, help='Key to filter Source Endpoint')
    parser.add_argument("--target_endpoint", required=True, type=str, help='Target Endpoint to search for key')
    parser.add_argument("--target_endpoint_filter_column", required=False, type=str, help='Target Endpoint filter column')
    parser.add_argument("--target_endpoint_filter_operator", required=False, choices=["eq", "ne"], help='Target Endpoint filter operator allows eq and ne, default is eq')
    parser.add_argument("--target_endpoint_filter_value", required=False, help='Target Endpoint filter value')
    args = parser.parse_args()

    main (
        source_endpoint=args.source_endpoint
        , year=args.year
        , key=args.key
        , target_endpoint=args.target_endpoint
        , target_endpoint_filter_column=args.target_endpoint_filter_column
        , target_endpoint_filter_operator=args.target_endpoint_filter_operator
        , target_endpoint_filter_value=args.target_endpoint_filter_value
    )