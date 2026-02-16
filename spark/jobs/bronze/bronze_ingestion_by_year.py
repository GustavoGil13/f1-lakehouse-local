import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Make sure spark/lib is importable
sys.path.append("/opt/spark/app_lib")

from openf1_client import fetch_json
from utils import json_serialize, setup_metadata_columns, setup_db_location
from logging_config import console_log_ingestion


def main(endpoint: str, year: int) -> None:
    spark = SparkSession.builder.appName(f"bronze_ingestion_{endpoint}").enableHiveSupport().getOrCreate()

    url, params_used, http_status, payload = fetch_json(endpoint, params={"year": year})

    # OpenF1 typically returns a JSON array. If it returns an object, we still store it as a single row.
    if isinstance(payload, list):
        raw_rows = [json_serialize(item) for item in payload]
    else:
        raw_rows = [json_serialize(payload)]

    df = spark.createDataFrame(raw_rows, "string").toDF("raw")

    request_id, ingestion_ts = setup_metadata_columns()

    df = (
        df.withColumn("ingestion_ts", F.lit(ingestion_ts))
          .withColumn("request_id", F.lit(request_id))
          .withColumn("source_url", F.lit(url))
          .withColumn("request_params", F.lit(json_serialize(params_used)))
          .withColumn("http_status", F.lit(http_status).cast("int"))
    )

    # Bronze write (append)
    bronze_db, bronze_db_location = setup_db_location("bronze")

    output_path = f"{bronze_db_location}/{endpoint}"

    # Register table in Hive metastore (idempotent)
    (
        df.write.format("delta")
        .mode("append")
        .option("path", output_path)
        .saveAsTable(f"{bronze_db}.{endpoint}")
    )

    console_log_ingestion(endpoint, df.count(), output_path, request_id)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Ingestion Information by Year")
    parser.add_argument("--endpoint", required=True, type=str, help='Endpoint to retrieve information, acts as table name in Bronze')
    parser.add_argument("--year", required=True, type=int, help='Year to filter Endpoint information')
    args = parser.parse_args()

    main(endpoint=args.endpoint, year=args.year)
