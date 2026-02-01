from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Make sure spark/lib is importable
import sys
sys.path.append("/opt/spark/jobs")   # jobs folder
sys.path.append("/opt/spark")        # sometimes useful
sys.path.append("/opt/project")      # if you mount project root (optional)
sys.path.append("/opt/spark/app_lib")    # not used here
sys.path.append("/opt/spark/jobs/..")

from openf1_client import fetch_json  # type: ignore
from utils import json_serialize
from logging_config import console_log_ingestion_ts, console_log_key, console_log_ingestion


def main(source_endpoint: str, year: int, key: str, target_endpoint: str) -> None:
    spark = SparkSession.builder.appName("bronze_ingestion_by_key").getOrCreate()

    source_delta_path = os.environ.get(f"BRONZE_{source_endpoint.upper()}_DELTA_PATH")
    target_delta_path = os.environ.get(f"BRONZE_{target_endpoint.upper()}_DELTA_PATH")

    # Read source data filtered by year
    session_filtered_by_year = (
            spark.read.format("delta")
            .load(source_delta_path)
            .withColumn("year", F.get_json_object("raw", "$.year").cast("int"))
            .withColumn(key, F.get_json_object("raw", f"$.{key}").cast("int"))
            .filter(F.col("year") == year)
            .select(
                "ingestion_ts"
                , "year"
                , key
            )
    )

    # Get most recent ingestion timestamp
    max_ingestion_ts = session_filtered_by_year.agg(F.max("ingestion_ts").alias("max_ingestion_ts")).collect()[0]["max_ingestion_ts"]
    most_recent_sessions = session_filtered_by_year.filter(F.col("ingestion_ts") == max_ingestion_ts)
    console_log_ingestion_ts(max_ingestion_ts, most_recent_sessions)

    # Get keys from most recent ingestion timestamp
    keys = most_recent_sessions.select(key).distinct().collect()
    keys_list = [row[key] for row in keys]
    console_log_key(key, keys_list)

    # Fetch endpoint data per key
    all_raw = []
    request_id = str(uuid.uuid4())
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    for k in keys_list:
        url, params_used, http_status, payload = fetch_json(target_endpoint, params={key: k})
        
        for item in payload:
            all_raw.append(
                {
                    "raw": json_serialize(item)
                    , "ingestion_ts": ingestion_ts
                    , "request_id": request_id
                    , "source_url": url
                    , "request_params": json_serialize(params_used)
                    , "http_status": http_status
                    , "year": year
                    , "sessions_ingestion_ts": max_ingestion_ts
                }
            )
    
    df = spark.createDataFrame(all_raw)
    
    df.write.format("delta").mode("append").save(target_delta_path)

    console_log_ingestion(target_endpoint, df, target_delta_path, request_id)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Ingestion Information by Key")
    parser.add_argument("--source_endpoint", required=True, help='Source Endpoint to retrieve list of keys')
    parser.add_argument("--year", required=True, help='Year to filter Source Endpoint')
    parser.add_argument("--key", required=True, help='Key to filter Source Endpoint')
    parser.add_argument("--target_endpoint", required=True, help='Target Endpoint to search for key')
    args = parser.parse_args()

    main(source_endpoint=args.source_endpoint, year=args.year, key=args.key, target_endpoint=args.target_endpoint)