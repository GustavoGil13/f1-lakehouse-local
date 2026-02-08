"""
Schema for the session result data.
"""

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, BooleanType

json_schema = StructType (
    [
        StructField("dnf", BooleanType())
        , StructField("dns", BooleanType())
        , StructField("driver_number", IntegerType())
        , StructField("dsq", BooleanType())
        , StructField("duration", FloatType())
        , StructField("gap_to_leader", StringType())
        , StructField("meeting_key", LongType())
        , StructField("number_of_laps", IntegerType())
        , StructField("points", FloatType())
        , StructField("position", IntegerType())
        , StructField("session_key", LongType())
    ]
)


if __name__ == "__main__":
    # Module intended for import; no CLI behavior.
    pass
