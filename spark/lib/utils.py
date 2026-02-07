import json
from typing import Any, Dict, List

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

if __name__ == "__main__":
    pass