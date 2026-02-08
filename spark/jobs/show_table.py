from __future__ import annotations

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main(os_path_var: str, table_name: str, year: int) -> None:
    spark = SparkSession.builder.appName("show_table").getOrCreate()

    path = os.environ.get(os_path_var) + table_name

    df = spark.read.format("delta").load(path).filter(f"year = {year}")

    df2 = df.filter("team_key = 764461175832650448")
    # df2 = df.filter("driver_key = -8517303739244913245")
    # df2 = df.groupBy("driver_key", "name_acronym").count().filter(F.col("count") > 1)

    print(df2.show(truncate=False, vertical=True))

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Silver Table")
    parser.add_argument("--os_path_var", required=True, help='Env var with path')
    parser.add_argument("--table_name", required=True, help='Table Name')
    parser.add_argument("--year", required=True, help='Year to filter Silver table')
    args = parser.parse_args()

    main(os_path_var=args.os_path_var, table_name=args.table_name, year=args.year)
