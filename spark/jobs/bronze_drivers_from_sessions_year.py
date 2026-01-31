from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

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
from utils import console_log_ingestion, json_serialize


def main(year: int) -> None:
    spark = SparkSession.builder.appName("bronze_drivers_from_sessions_year").getOrCreate()

    sessions_delta_path = os.environ.get("BRONZE_SESSIONS_DELTA_PATH")
    drivers_delta_path = os.environ.get("BRONZE_DRIVERS_DELTA_PATH")

    # Read Session data filtered by year
    session_filtered_by_year = (
            spark.read.format("delta")
            .load(sessions_delta_path)
            .withColumn("year", F.get_json_object("raw", "$.year").cast("int"))
            .withColumn("meeting_key", F.get_json_object("raw", "$.meeting_key").cast("int"))
            .filter(F.col("year") == year)
            .select(
                "ingestion_ts"
                , "year"
                , "meeting_key"
            )
    )

    # Get most recent ingestion timestamp
    max_ingestion_ts = session_filtered_by_year.agg(F.max("ingestion_ts").alias("max_ingestion_ts")).collect()[0]["max_ingestion_ts"]
    # print(max_ingestion_ts, most_recent_sessions.count())
    most_recent_sessions = session_filtered_by_year.filter(F.col("ingestion_ts") == max_ingestion_ts)
    # print(most_recent_sessions.show(5))

    # Get meeting keys from most recent ingestion timestamp
    meeting_keys = most_recent_sessions.select("meeting_key").distinct().collect()
    meeting_keys_list = [row["meeting_key"] for row in meeting_keys]
    # print(meeting_keys_list)

    # Fetch drivers data per meeting key
    all_raw = []
    request_id = str(uuid.uuid4())
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    for meeting_key in meeting_keys_list:
        url, params_used, http_status, payload = fetch_json("drivers", params={"meeting_key": meeting_key})
        
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
    
    df.write.format("delta").mode("append").save(drivers_delta_path)

    console_log_ingestion("drivers", df, drivers_delta_path, request_id)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Ingestion: Driver Information")
    parser.add_argument("--year", required=True, help='Year to search Sessions to get meeting keys')
    args = parser.parse_args()

    main(year=args.year)