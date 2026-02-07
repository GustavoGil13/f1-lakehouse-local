import json
from typing import Any, Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from logging_config import console_log_ingestion_ts

def json_serialize(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

def extract_failed_expectations(result) -> List[Dict[str, Any]]:
    result_dict = result.to_json_dict() if hasattr(result, "to_json_dict") else dict(result)

    failed: List[Dict[str, Any]] = []
    for r in result_dict.get("results", []):
        if not r.get("success", True):
            failed.append({
                "expectation_type": r.get("expectation_config", {}).get("expectation_type"),
                "kwargs": r.get("expectation_config", {}).get("kwargs", {}),
                "result": r.get("result", {}),
                "exception_info": r.get("exception_info", {}),
            })
    return failed

def failed_expectations_json(result) -> str:
    """Atalho: devolve as failed expectations em JSON string."""
    return json.dumps(extract_failed_expectations(result), default=str)


def get_most_recent_data(df: DataFrame, filter_column: str) -> DataFrame:
    max_ingestion_ts = df.agg(F.max(filter_column).alias(f"max_{filter_column}")).collect()[0][f"max_{filter_column}"]
    most_recent_data = df.filter(F.col(filter_column) == max_ingestion_ts)
    console_log_ingestion_ts(max_ingestion_ts, most_recent_data)
    return most_recent_data


def gmt_offset_to_seconds(offset_col: Column) -> Column:
    sign = F.when(offset_col.startswith("-"), -1).otherwise(1)
    hours = F.abs(F.split(offset_col, ":")[0].cast("int"))
    mins  = F.split(offset_col, ":")[1].cast("int")
    secs  = F.split(offset_col, ":")[2].cast("int")
    return sign * (hours * 3600 + mins * 60 + secs)


def apply_gmt_offset(ts_col: Column, offset_col: Column) -> Column:
    return F.from_unixtime (
        F.unix_timestamp(ts_col) - gmt_offset_to_seconds(offset_col)
    ).cast("timestamp")


if __name__ == "__main__":
    pass