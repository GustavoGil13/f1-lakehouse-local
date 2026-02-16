import argparse
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("/opt/spark/app_lib")

from utils import setup_db_location


def main(table_name: str, year: int) -> None:
    spark = SparkSession.builder.appName("dq_failed").enableHiveSupport().getOrCreate()

    db, _ = setup_db_location("dq")

    df = (
        spark.table(f"{db}.dq_runs")
        .filter(f"year = {year} and table = '{table_name}' and success is False")
        .orderBy(F.desc("run_ts"))
    )

    failed_json = df.select("failed_json").first()[0]

    print(df.show(1, truncate=False, vertical=True))

    print(json.dumps(json.loads(failed_json), indent=2))

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Silver Table")
    parser.add_argument("--table_name", required=True, type=str, help='Table Name')
    parser.add_argument("--year", required=True, type=int, help='Year to filter Silver table')
    args = parser.parse_args()

    main(table_name=args.table_name, year=args.year)
