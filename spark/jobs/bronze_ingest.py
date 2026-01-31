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
    env_key = f"BRONZE_{endpoint.upper()}_DELTA_PATH"
    value = os.environ.get(env_key)
    if not value:
        raise RuntimeError(
            f"Missing env var {env_key}. "
            f"Define it in .env (and pass it via docker-compose environment)."
        )
    return value


import json
import os
from typing import Any, Dict, Optional


def load_bronze_params(params_filename: Optional[str], base_dir: str = "/opt/spark/jobs") -> Dict[str, Any]:
    """
    Load Bronze ingestion params from a JSON file.

    If params_filename is None -> return {}
    If file does not exist -> raise error
    """
    if not params_filename:
        print("No --params provided. Proceeding with empty params.")
        return {}

    params_path = os.path.join(base_dir, params_filename)

    if not os.path.exists(params_path):
        raise RuntimeError(f"Params file not found: {params_path}")

    try:
        with open(params_path, "r", encoding="utf-8") as f:
            params = json.load(f)

        if not isinstance(params, dict):
            raise RuntimeError("Params file must contain a JSON object")

        print(f"Loaded params from {params_path}: {params}")
        return params

    except Exception as e:
        raise RuntimeError(f"Failed to load params from {params_path}: {e}") from e



def main(endpoint: str, params_json: Optional[str]) -> None:
    spark = SparkSession.builder.appName(f"bronze_ingest_{endpoint}").getOrCreate()

    request_id = str(uuid.uuid4())
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    params: Dict[str, Any] = load_bronze_params(params_json)

    output_path = env_output_path_for(endpoint)

    url, params_used, http_status, payload = fetch_json(endpoint, params=params)

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
    parser = argparse.ArgumentParser(description="Generic OpenF1 -> Bronze Delta ingestor")
    parser.add_argument("--endpoint", required=True, help='OpenF1 endpoint name, e.g. "sessions" or "drivers"')
    parser.add_argument(
        "--params",
        required=False,
        help="Filename of a JSON file with query params (located in spark/jobs)",
    )
    args = parser.parse_args()

    main(endpoint=args.endpoint, params_json=args.params)
