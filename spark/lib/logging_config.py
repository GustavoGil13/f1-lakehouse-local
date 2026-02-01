import logging
from pyspark.sql import DataFrame
from typing import List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def console_log_ingestion(endpoint: str, df: DataFrame, delta_path: str, request_id: str) -> None:
    logging.info(f"OK: endpoint={endpoint} rows={df.count()} output={delta_path} request_id={request_id}")

def console_log_check(table_name: str, path: str, df: DataFrame) -> None:
    logging.info(f"OK: endpoint={table_name} path={path} rows={df.count()}")

def console_log_ingestion_ts(ts: str, df: DataFrame) -> None:
    logging.info(f"OK: max_ingestion_ts={ts}, rows={df.count()}")

def console_log_key(key: str, keys_list: List) -> None:
    logging.info(f"OK: {key}_list_length={len(keys_list)}")
