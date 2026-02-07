import argparse
import os
import sys

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from great_expectations.dataset import SparkDFDataset

sys.path.append("/opt/spark/app_lib")

from utils import failed_expectations_json
from logging_config import console_log_dq

def main(year: int):
    silver_table_name = "circuits"

    spark = SparkSession.builder.appName(f"silver_data_quality_{silver_table_name}").getOrCreate()

    silver_path = os.environ.get("SILVER_DELTA_PATH") + silver_table_name

    df = spark.read.format("delta").load(silver_path).where(f"year = {year}")

    gdf = SparkDFDataset(df)

    # -------------------------------------------------------
    gdf.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    gdf.expect_column_values_to_not_be_null("circuit_key")
    gdf.expect_column_values_to_be_unique("circuit_key")
    gdf.expect_column_values_to_not_be_null("circuit_short_name")
    gdf.expect_column_values_to_not_be_null("circuit_type")
    # -------------------------------------------------------

    result = gdf.validate()
    failed_json = failed_expectations_json(result)

    request_id = df.select("request_id").distinct().first()[0]
    run_ts = df.select("run_ts").distinct().first()[0]

    success = bool(result["success"])
    dq_run_ts = datetime.now(timezone.utc).isoformat()


    dq_runs_df = spark.createDataFrame (
        [
            ("silver", silver_table_name, year, request_id, run_ts, dq_run_ts, success, failed_json)
        ]
        , ["layer", "table", "year", "request_id", "run_ts", "dq_run_ts", "success", "failed_json"]
    )

    # print(dq_runs_df.show(1, truncate=False, vertical=True))

    dq_runs_path = os.environ.get("DQ_DELTA_PATH") + "dq_runs"
    dq_runs_df.write.format("delta").mode("append").save(dq_runs_path)

    spark.stop()

    if success:
        console_log_dq(silver_table_name, silver_path)
    else:
        raise SystemExit("DQ FAILED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Circuits Data Quality")
    parser.add_argument("--year", required=True, help='Year to filter')
    args = parser.parse_args()

    main(year=args.year)
