import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from great_expectations.dataset import SparkDFDataset

sys.path.append("/opt/spark/app_lib")

from utils import failed_expectations_json, setup_metadata_columns
from logging_config import console_log_dq

from dq_expectations import EXPECTATIONS_REGISTRY


def main(table: str, year: int):
    spark = SparkSession.builder.appName(f"silver_data_quality_{table}").getOrCreate()

    silver_path = os.environ.get("SILVER_DELTA_PATH") + table
    df = spark.read.format("delta").load(silver_path).where(f"year = {year}")

    gdf = SparkDFDataset(df)

    try:
        EXPECTATIONS_REGISTRY[table](gdf)
    except KeyError:
        raise SystemExit(f"No expectations registered for this table: {table}")

    result = gdf.validate()
    failed_json = failed_expectations_json(result)

    request_id = df.select("request_id").distinct().first()[0]
    run_ts = df.select("run_ts").distinct().first()[0]
    success = bool(result["success"])
    _, dq_run_ts = setup_metadata_columns()

    dq_runs_df = spark.createDataFrame(
        [("silver", table, year, request_id, run_ts, dq_run_ts, success, failed_json)],
        ["layer", "table", "year", "request_id", "run_ts", "dq_run_ts", "success", "failed_json"],
    )

    dq_runs_path = os.environ.get("DQ_DELTA_PATH") + "dq_runs"
    dq_runs_df.write.format("delta").mode("append").save(dq_runs_path)

    spark.stop()

    if success:
        console_log_dq(table, silver_path)
    else:
        raise SystemExit("DQ FAILED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Data Quality Runner")
    parser.add_argument("--table", required=True, type=str, help="Silver Table Name (ex: circuits)")
    parser.add_argument("--year", required=True, type=int, help="Year to filter")
    args = parser.parse_args()

    main(table=args.table, year=args.year)
