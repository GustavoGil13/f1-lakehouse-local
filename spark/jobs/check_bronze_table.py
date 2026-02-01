from __future__ import annotations

import argparse
import os
from pyspark.sql import SparkSession



def main(table_name: str) -> None:
    spark = SparkSession.builder.appName(f"check_bronze_table").getOrCreate()

    path = os.environ.get(f"BRONZE_{table_name.upper()}_DELTA_PATH")
    df = spark.read.format("delta").load(path)

    print(f"OK: endpoint={table_name} path={path}")
    print("rows =", df.count())

    df.groupBy("ingestion_ts").count().orderBy("ingestion_ts").show(10, False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Bronze Delta table by name")
    parser.add_argument("--table_name", required=True, help='Table name, e.g. "sessions"')
    args = parser.parse_args()

    main(table_name=args.table_name)
