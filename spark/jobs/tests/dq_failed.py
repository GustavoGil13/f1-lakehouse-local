from __future__ import annotations

import argparse
import os
import json
from pyspark.sql import SparkSession

def main(table_name: str, year: int) -> None:
    spark = SparkSession.builder.appName("dq_failed").getOrCreate()

    path = os.environ.get("DQ_DELTA_PATH") + "dq_runs"

    df = spark.read.format("delta").load(path).filter(f"year = {year} and table = '{table_name}' and success is False")

    failed_json = df.select("failed_json").first()[0]

    print(df.show(5, truncate=False, vertical=True))

    print(json.dumps(json.loads(failed_json), indent=2))

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Silver Table")
    parser.add_argument("--table_name", required=True, help='Table Name')
    parser.add_argument("--year", required=True, help='Year to filter Silver table')
    args = parser.parse_args()

    main(table_name=args.table_name, year=args.year)
