"""
Schema for the ingestion by year data.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

json_schema = StructType (
    [
        StructField("raw", StringType())
        , StructField("ingestion_ts", StringType())
        , StructField("request_id", StringType())
        , StructField("source_url", StringType())
        , StructField("request_params", StringType())
        , StructField("http_status", IntegerType())
        , StructField("year", IntegerType())
        , StructField("source_endpoint_ingestion_ts", StringType())
    ]
)