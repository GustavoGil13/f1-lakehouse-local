from __future__ import annotations

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def env_output_path_for(endpoint: str) -> str:
    value = os.environ.get("BRONZE_DELTA_PATH") + endpoint
    if not value:
        raise RuntimeError(
            f"Missing env var. "
            f"Define it in .env (and pass it via docker-compose environment)."
        )
    return value


def main(endpoint: str) -> None:
    spark = SparkSession.builder.appName(f"check_bronze_{endpoint}").getOrCreate()

    path = env_output_path_for(endpoint)
    df = spark.read.format("delta").load(path)

    print(f"OK: endpoint={endpoint} path={path}")
    print("rows =", df.count())

    df.groupBy("ingestion_ts").count().orderBy("ingestion_ts").show(10, False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Bronze Delta table by endpoint env var")
    parser.add_argument("--endpoint", required=True, help='Endpoint name, e.g. "sessions"')
    args = parser.parse_args()

    main(endpoint=args.endpoint)
