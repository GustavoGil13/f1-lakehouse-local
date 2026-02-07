from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

json_schema = StructType (
    [
        StructField("broadcast_name", StringType())
        , StructField("country_code", StringType())
        , StructField("driver_number", IntegerType())
        , StructField("first_name", StringType())
        , StructField("full_name", StringType())
        , StructField("headshot_url", StringType())
        , StructField("last_name", StringType())
        , StructField("meeting_key", LongType())
        , StructField("name_acronym", StringType())
        , StructField("session_key", LongType())
        , StructField("team_colour", StringType())
        , StructField("team_name", StringType())
    ]
)
