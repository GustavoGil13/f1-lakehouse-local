import argparse
import sys

from pyspark.sql import SparkSession
from great_expectations.dataset import SparkDFDataset

sys.path.append("/opt/spark/app_lib")

from utils import failed_expectations_json, setup_metadata_columns, setup_db_location, create_db_if_not_exists
from logging_config import console_log_dq

from dq_expectations import EXPECTATIONS_REGISTRY


def main(db_name: str, table_name: str, year: int):
    spark = SparkSession.builder.appName(f"{db_name}_data_quality_{table_name}").enableHiveSupport().getOrCreate()

    db, db_location = setup_db_location(db_name)
    df = spark.table(f"{db}.{table_name}").where(f"year = {year}")

    gdf = SparkDFDataset(df)

    try:
        EXPECTATIONS_REGISTRY[table_name](gdf)
    except KeyError:
        raise SystemExit(f"No expectations registered for this table: {table_name}")

    result = gdf.validate()
    failed_json = failed_expectations_json(result)

    request_id = df.select("request_id").distinct().first()[0]
    run_ts = df.select("run_ts").distinct().first()[0]
    success = bool(result["success"])
    _, dq_run_ts = setup_metadata_columns()

    dq_runs_df = spark.createDataFrame(
        [(db_name, table_name, year, request_id, run_ts, dq_run_ts, success, failed_json)],
        ["layer", "table", "year", "request_id", "run_ts", "dq_run_ts", "success", "failed_json"],
    )

    dq_db, dq_db_location = setup_db_location("dq")

    # create database if not exists with location (idempotent)
    create_db_if_not_exists(spark, dq_db, dq_db_location)

    output_path = f"{dq_db_location}/dq_runs"

        # Register table in Hive metastore (idempotent)
    (
        dq_runs_df.write.format("delta")
        .mode("append")
        .option("path", output_path)
        .saveAsTable(f"{dq_db}.dq_runs")
    )

    spark.stop()

    if success:
        console_log_dq(table_name, f"{db_location}/{table_name}")
    else:
        raise SystemExit("DQ FAILED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Quality Runner")
    parser.add_argument("--db_name", required=True, type=str, help="Database Name (ex: silver)")
    parser.add_argument("--table_name", required=True, type=str, help="Table Name (ex: circuits)")
    parser.add_argument("--year", required=True, type=int, help="Year to filter")
    args = parser.parse_args()

    main(db_name=args.db_name, table_name=args.table_name, year=args.year)
