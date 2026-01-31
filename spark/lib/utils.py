import json
from pyspark.sql import DataFrame


def json_serialize(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def console_log_ingestion(endpoint: str, df: DataFrame, delta_path: str, request_id: str) -> None:
    print(f"OK: endpoint={endpoint} rows={df.count()} output={delta_path} request_id={request_id}")


if __name__ == "__main__":
    pass