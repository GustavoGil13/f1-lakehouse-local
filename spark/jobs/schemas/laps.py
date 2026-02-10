"""
Schema for the sessions data.
"""

from pyspark.sql.types import StructType, StructField, ArrayType, LongType, IntegerType, TimestampType, FloatType, BooleanType

json_schema = StructType (
    [
        StructField("date_start", TimestampType())
        , StructField("driver_number", IntegerType())
        , StructField("duration_sector_1", FloatType())
        , StructField("duration_sector_2", FloatType())
        , StructField("duration_sector_3", FloatType())
        , StructField("i1_speed", IntegerType())
        , StructField("i2_speed", IntegerType())
        , StructField("is_pit_out_lap", BooleanType())
        , StructField("lap_duration", FloatType())
        , StructField("lap_number", IntegerType())
        , StructField("meeting_key", LongType())
        , StructField("segments_sector_1", ArrayType(IntegerType()))
        , StructField("segments_sector_2", ArrayType(IntegerType()))
        , StructField("segments_sector_3", ArrayType(IntegerType()))
        , StructField("session_key", LongType())
        , StructField("st_speed", IntegerType())
    ]
)

sector_map = {
    0: "not available"
    , 2048: "yellow sector"
    , 2049: "green sector"
    , 2050: "?"
    , 2051: "purple sector"
    , 2052: "?"
    , 2064: "pitlane"
    , 2068: "?"
}


if __name__ == "__main__":
    # Module intended for import; no CLI behavior.
    pass
