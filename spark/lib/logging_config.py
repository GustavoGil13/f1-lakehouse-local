"""
Logging helpers for Spark jobs.

Purpose:
- Centralize simple console logging used by ingestion/check/DQ jobs.
- Keep messages consistent and easy to grep in container logs.
"""
import logging
from pyspark.sql import DataFrame
from typing import List

# Basic console logger configuration for local/dev runs.
# Adjust level/format or replace with log4j2 when running inside Spark containers.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def console_log_ingestion(endpoint: str, count: int, delta_path: str, request_id: str) -> None:
    """
    Log ingestion result.

    Args:
        endpoint: logical source name or API endpoint.
        count: number of rows written.
        delta_path: destination path (Delta table) written to.
        request_id: request or job identifier for tracing.
    """
    logging.info(f"OK: endpoint={endpoint} rows={count} output={delta_path} request_id={request_id}")

def console_log_check(table_name: str, path: str, df: DataFrame) -> None:
    """
    Log a quick data check (rows) for a table or path.

    Args:
        table_name: table identifier.
        path: storage path checked.
        df: DataFrame representing the table content.
    """
    logging.info(f"OK: endpoint={table_name} path={path} rows={df.count()}")

def console_log_ingestion_ts(ts: str, df: DataFrame) -> None:
    """
    Log ingestion timestamp validation results.

    Args:
        ts: max ingestion timestamp observed (string or ISO timestamp).
        df: DataFrame used to compute/validate the timestamp.
    """
    logging.info(f"OK: max_ingestion_ts={ts}, rows={df.count()}")

def console_log_key(key: str, keys_list: List) -> None:
    """
    Log the number of keys collected/processed.

    Args:
        key: key name or type.
        keys_list: iterable/list of keys (kept generic to accept different types).
    """
    logging.info(f"OK: {key}_list_length={len(keys_list)}")

def console_log_dq(endpoint: str, path:str) -> None:
    """
    Log a successful data quality run.

    Args:
        endpoint: logical name of the dataset/check.
        path: storage path where the dataset/check results live.
    """
    logging.info(f"OK - ALL TEST SUCCEEDED: endpoint={endpoint} path={path}")


if __name__ == "__main__":
    # Module intended for import; no CLI behavior.
    pass
