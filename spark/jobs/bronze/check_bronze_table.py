from __future__ import annotations

import argparse
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append("/opt/spark/app_lib")

from logging_config import console_log_check

def main(table_name: str, groupBy_key=None) -> None:
    spark = SparkSession.builder.appName(f"check_bronze_table").getOrCreate()

    path = os.environ.get(f"BRONZE_{table_name.upper()}_DELTA_PATH")

    df = spark.read.format("delta").load(path)

    if groupBy_key:
        (
            df.withColumn(groupBy_key, F.get_json_object("raw", f"$.{groupBy_key}").cast("int"))
            .groupBy("ingestion_ts", groupBy_key).count().orderBy("ingestion_ts").show(10, False)
        )
    else:
        df.groupBy("ingestion_ts").count().orderBy("ingestion_ts").show(10, False)

    console_log_check(table_name, path, df)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check a Bronze Delta table by name")
    parser.add_argument("--table_name", required=True, help='Table name, e.g. "sessions"')
    parser.add_argument("--groupBy_key", required=False, help='Key for groupBy, e.g. "meeting_key"')
    args = parser.parse_args()

    main(table_name=args.table_name, groupBy_key=args.groupBy_key)
