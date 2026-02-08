"""
Utility helpers for Spark jobs.

"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from logging_config import console_log_ingestion_ts

def json_serialize(obj: dict) -> str:
    """
    Serialize a dict to a compact JSON string.
    """
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

def setup_metadata_columns():
    """
    Generate a new request_id (UUID) and ingestion_ts (current UTC timestamp in ISO format).
    """
    return str(uuid.uuid4()), datetime.now(timezone.utc).isoformat()

def extract_failed_expectations(result) -> List[Dict[str, Any]]:
    """
    Extract failed expectations from a Great Expectations result-like object.

    Args:
        result: an object with `to_json_dict()` or a dict-like GE result.

    Returns:
        List of dicts with keys: expectation_type, kwargs, result, exception_info.
    """
    result_dict = result.to_json_dict() if hasattr(result, "to_json_dict") else dict(result)

    failed: List[Dict[str, Any]] = []
    for r in result_dict.get("results", []):
        if not r.get("success", True):
            failed.append (
                {
                    "expectation_type": r.get("expectation_config", {}).get("expectation_type")
                    , "kwargs": r.get("expectation_config", {}).get("kwargs", {})
                    , "result": r.get("result", {})
                    , "exception_info": r.get("exception_info", {})
                }
            )
    return failed

def failed_expectations_json(result) -> str:
    """Shortcut: return failed expectations as a JSON string (safe default=str)."""
    return json.dumps(extract_failed_expectations(result), default=str)


def get_most_recent_data(df: DataFrame, filter_column: str, get_max_ingestion_ts: bool = False) -> DataFrame:
    """
    Given a DataFrame and a filter column (e.g. ingestion_ts), return the subset of data with the most recent timestamp.

    If get_max_ingestion_ts is True, also return the max ingestion timestamp as a string.
    """
    max_ingestion_ts = df.agg(F.max(filter_column).alias(f"max_{filter_column}")).collect()[0][f"max_{filter_column}"]
    most_recent_data = df.filter(F.col(filter_column) == max_ingestion_ts)
    console_log_ingestion_ts(max_ingestion_ts, most_recent_data)

    if get_max_ingestion_ts:
        return most_recent_data, max_ingestion_ts
    
    return most_recent_data


def gmt_offset_to_seconds(offset_col: Column) -> Column:
    """
    Convert a GMT offset column (string) to an integer seconds Column.

    Expected format: [+|-]HH:MM:SS or HH:MM:SS (assumes positive).
    Returns a Column with signed seconds (int).
    """
    sign = F.when(offset_col.startswith("-"), -1).otherwise(1)
    hours = F.abs(F.split(offset_col, ":")[0].cast("int"))
    mins  = F.split(offset_col, ":")[1].cast("int")
    secs  = F.split(offset_col, ":")[2].cast("int")
    return sign * (hours * 3600 + mins * 60 + secs)


def apply_gmt_offset(ts_col: Column, offset_col: Column) -> Column:
    """
    Apply a GMT offset to a timestamp column, returning a UTC-aligned timestamp Column.

    """
    return F.from_unixtime(F.unix_timestamp(ts_col) - gmt_offset_to_seconds(offset_col)).cast("timestamp")


if __name__ == "__main__":
    # Module intended for import; no CLI behavior.
    pass
