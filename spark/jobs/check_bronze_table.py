from __future__ import annotations

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def env_output_path_for(name: str) -> str:
    value = os.environ.get("BRONZE_DELTA_PATH") + name
    if not value:
        raise RuntimeError(
            f"Missing env var. "
            f"Define it in .env (and pass it via docker-compose environment)."
        )
    return value


def main(name: str) -> None:
    spark = SparkSession.builder.appName(f"check_bronze_table").getOrCreate()

    path = env_output_path_for(name)
    df = spark.read.format("delta").load(path)

    print(f"OK: endpoint={name} path={path}")
    print("rows =", df.count())

    df.groupBy("ingestion_ts").count().orderBy("ingestion_ts").show(10, False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Bronze Delta table by name")
    parser.add_argument("--name", required=True, help='Table name, e.g. "sessions"')
    args = parser.parse_args()

    main(name=args.name)
