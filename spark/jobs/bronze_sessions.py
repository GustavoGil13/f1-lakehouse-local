import os
import argparse
import json
import uuid
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, lit, sha2, to_date


OPENF1_BASE = "https://api.openf1.org/v1"
OUTPUT_PATH = os.environ.get("BRONZE_SESSIONS_DELTA_PATH")
if not OUTPUT_PATH:
    raise RuntimeError("Environment variable BRONZE_SESSIONS_DELTA_PATH is not set")


def fetch_sessions(year: int):
    url = f"{OPENF1_BASE}/sessions"
    params = {"year": year}
    r = requests.get(url, params=params, timeout=30)
    status = r.status_code
    r.raise_for_status()
    return url, params, status, r.json()


def main(year: int):

    spark = SparkSession.builder.appName("openf1_bronze_sessions").getOrCreate()

    request_id = str(uuid.uuid4())
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    url, params, http_status, items = fetch_sessions(year)

    # print("request_id:", request_id)
    # print("ingestion_ts:", ingestion_ts)
    # print("url:", url)
    # print("params:", params)
    # print("http_status:", http_status)

    # 1 row per session, stored as raw JSON string
    raw_rows = [json.dumps(item, separators=(",", ":"), ensure_ascii=False) for item in items]
    # print("raw_rows:", raw_rows[0])
    df = spark.createDataFrame(raw_rows, "string").toDF("raw")

    # Bronze "envelope" columns
    df = (
        df.withColumn("endpoint", lit("sessions"))
          .withColumn("ingestion_ts", lit(ingestion_ts))
          .withColumn("ingest_date", to_date(col("ingestion_ts")))
          .withColumn("year", lit(int(year)))
          .withColumn("request_id", lit(request_id))
          .withColumn("source_url", lit(url))
          .withColumn("request_params", lit(json.dumps(params)))
          .withColumn("http_status", lit(int(http_status)))
          .withColumn("record_hash", sha2(col("raw"), 256))
          # optional convenience field (helps later)
          .withColumn("session_key", get_json_object(col("raw"), "$.session_key").cast("int"))
    )

    # print(df.head())

    df.write.format("delta").mode("append").save(OUTPUT_PATH)

    print(f"OK: wrote {df.count()} rows to {OUTPUT_PATH} (year={year})")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()

    main(args.year)
