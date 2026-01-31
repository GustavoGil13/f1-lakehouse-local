from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, to_date

# Make sure spark/lib is importable
import sys
sys.path.append("/opt/spark/jobs")   # jobs folder
sys.path.append("/opt/spark")        # sometimes useful
sys.path.append("/opt/project")      # if you mount project root (optional)
sys.path.append("/opt/spark/app_lib")    # not used here
sys.path.append("/opt/spark/jobs/..")

from openf1_client import fetch_json  # type: ignore


def env_output_path_for(endpoint: str) -> str:
    value = os.environ.get("BRONZE_DELTA_PATH") + endpoint
    if not value:
        raise RuntimeError(
            f"Missing env var. "
            f"Define it in .env (and pass it via docker-compose environment)."
        )
    return value


def main(endpoint: str, year: int) -> None:
    spark = SparkSession.builder.appName(f"bronze_ingest_{endpoint}").getOrCreate()

    request_id = str(uuid.uuid4())
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    output_path = env_output_path_for(endpoint)

    url, params_used, http_status, payload = fetch_json(endpoint, params={"year": year})

    # OpenF1 typically returns a JSON array. If it returns an object, we still store it as a single row.
    if isinstance(payload, list):
        raw_rows = [json.dumps(item, separators=(",", ":"), ensure_ascii=False) for item in payload]
    else:
        raw_rows = [json.dumps(payload, separators=(",", ":"), ensure_ascii=False)]

    df = spark.createDataFrame(raw_rows, "string").toDF("raw")

    df = (
        df.withColumn("endpoint", lit(endpoint))
          .withColumn("ingestion_ts", lit(ingestion_ts))
          .withColumn("ingest_date", to_date(col("ingestion_ts")))
          .withColumn("request_id", lit(request_id))
          .withColumn("source_url", lit(url))
          .withColumn("request_params", lit(json.dumps(params_used, separators=(",", ":"), ensure_ascii=False)))
          .withColumn("http_status", lit(int(http_status)))
          .withColumn("record_hash", sha2(col("raw"), 256))
    )

    # Bronze write (append)
    df.write.format("delta").mode("append").save(output_path)

    print(f"OK: endpoint={endpoint} rows={df.count()} output={output_path} request_id={request_id}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Ingestion: Session Information")
    parser.add_argument("--year", required=True, help='Year to retrieve Session information')
    args = parser.parse_args()

    main(endpoint="sessions", year=args.year)
