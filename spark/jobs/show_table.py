from __future__ import annotations

import argparse
import os
from pyspark.sql import SparkSession

def main(os_path_var: str, year: int) -> None:
    spark = SparkSession.builder.appName("show_table").getOrCreate()

    path = os.environ.get(os_path_var)

    df = spark.read.format("delta").load(path).filter(f"year = {year}")

    print(df.show(5, truncate=False, vertical=True))

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Bronze Delta table by name")
    parser.add_argument("--os_path_var", required=True, help='Env var with path')
    parser.add_argument("--year", required=True, help='Year to filter Bronze table')
    args = parser.parse_args()

    main(os_path_var=args.os_path_var, year=args.year)
