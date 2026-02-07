from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, to_date

# Make sure spark/lib is importable
import sys
sys.path.append("/opt/spark/jobs")   # jobs folder
sys.path.append("/opt/spark")        # sometimes useful
sys.path.append("/opt/project")      # if you mount project root (optional)
sys.path.append("/opt/spark/app_lib")
sys.path.append("/opt/spark/jobs/..")

from openf1_client import fetch_json  # type: ignore
from utils import json_serialize
from logging_config import console_log_ingestion


def main(endpoint: str, year: int) -> None:
    spark = SparkSession.builder.appName(f"bronze_ingest_{endpoint}").getOrCreate()

    request_id = str(uuid.uuid4())
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    output_path = os.environ.get(f"BRONZE_{endpoint.upper()}_DELTA_PATH")

    url, params_used, http_status, payload = fetch_json(endpoint, params={"year": year})

    # OpenF1 typically returns a JSON array. If it returns an object, we still store it as a single row.
    if isinstance(payload, list):
        raw_rows = [json_serialize(item) for item in payload]
    else:
        raw_rows = [json_serialize(payload)]

    df = spark.createDataFrame(raw_rows, "string").toDF("raw")

    df = (
        df.withColumn("ingestion_ts", lit(ingestion_ts))
          .withColumn("request_id", lit(request_id))
          .withColumn("source_url", lit(url))
          .withColumn("request_params", lit(json.dumps(params_used, separators=(",", ":"), ensure_ascii=False)))
          .withColumn("http_status", lit(int(http_status)))
    )

    # Bronze write (append)
    df.write.format("delta").mode("append").save(output_path)

    console_log_ingestion(endpoint, df, output_path, request_id)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Ingestion Information by Year")
    parser.add_argument("--endpoint", required=True, help='Endpoint to retrieve information')
    parser.add_argument("--year", required=True, help='Year to filter Endpoint information')
    args = parser.parse_args()

    main(endpoint=args.endpoint, year=args.year)
