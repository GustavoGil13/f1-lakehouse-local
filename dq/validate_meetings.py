import argparse
import os

from pyspark.sql import SparkSession
from great_expectations.dataset import SparkDFDataset

def main(year: int):

    spark = SparkSession.builder.appName("silver_data_quality_meetings").getOrCreate()

    silver_path = os.environ.get("SILVER_MEETINGS_DELTA_PATH")

    df = spark.read.format("delta").load(silver_path).where(f"year = {year}")

    gdf = SparkDFDataset(df)

    # -------------------------------------------------------
    gdf.expect_table_row_count_to_be_between(min_value=1, max_value=None)
    gdf.expect_column_values_to_not_be_null("meeting_key")
    gdf.expect_column_values_to_be_unique("meeting_key")
    gdf.expect_column_values_to_not_be_null("local_ts_start")
    gdf.expect_column_pair_values_A_to_be_greater_than_B("local_ts_end", "local_ts_start", or_equal=True)
    gdf.expect_column_values_to_not_be_null("ts_start")
    gdf.expect_column_pair_values_A_to_be_greater_than_B("ts_end", "ts_start", or_equal=True)
    gdf.expect_column_values_to_match_regex("country_code", r"^[A-Z]{3}$")
    gdf.expect_column_values_to_match_regex("gmt_offset", r"^[+-]?\d{2}:\d{2}:\d{2}$")
    # -------------------------------------------------------

    result = gdf.validate()

    if not result["success"]:
        raise SystemExit("DQ FAILED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Data Quality")
    parser.add_argument("--year", required=True, help='Year to filter')
    args = parser.parse_args()

    main(year=args.year)
