from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType

json_schema = StructType (
    [
        StructField("circuit_key", LongType())
        , StructField("circuit_short_name", StringType())
        , StructField("country_code", StringType())
        , StructField("country_key", LongType())
        , StructField("country_name", StringType())
        , StructField("date_end", TimestampType())
        , StructField("date_start", TimestampType())
        , StructField("gmt_offset", StringType())
        , StructField("location", StringType())
        , StructField("meeting_key", LongType())
        , StructField("session_key", LongType())
        , StructField("session_name", StringType())
        , StructField("session_type", StringType())
        , StructField("year", IntegerType())
    ]
)
