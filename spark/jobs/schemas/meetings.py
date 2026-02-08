"""
Schema for the meetings data.
"""

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType

json_schema = StructType (
    [
        StructField("circuit_image", StringType())
        , StructField("circuit_info_url", StringType())
        , StructField("circuit_key", LongType())
        , StructField("circuit_short_name", StringType())
        , StructField("circuit_type", StringType())
        , StructField("country_code", StringType())
        , StructField("country_flag", StringType())
        , StructField("country_key", LongType())
        , StructField("country_name", StringType())
        , StructField("date_end", TimestampType())
        , StructField("date_start", TimestampType())
        , StructField("gmt_offset", StringType())
        , StructField("location", StringType())
        , StructField("meeting_key", LongType())
        , StructField("meeting_name", StringType())
        , StructField("meeting_official_name", StringType())
        , StructField("year", IntegerType())
    ]
)


if __name__ == "__main__":
    # Module intended for import; no CLI behavior.
    pass
