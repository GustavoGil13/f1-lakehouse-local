from __future__ import annotations

import argparse
import os
from pyspark.sql import SparkSession

def main(os_path_var: str, table_name: str, year: int) -> None:
    spark = SparkSession.builder.appName("show_table").getOrCreate()

    path = os.environ.get(os_path_var) + table_name

    df = spark.read.format("delta").load(path).filter(f"year = {year}")

    print(df.show(5, truncate=False, vertical=True))

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Silver Table")
    parser.add_argument("--os_path_var", required=True, help='Env var with path')
    parser.add_argument("--table_name", required=True, help='Table Name')
    parser.add_argument("--year", required=True, help='Year to filter Silver table')
    args = parser.parse_args()

    main(os_path_var=args.os_path_var, table_name=args.table_name, year=args.year)
