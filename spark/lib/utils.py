import json

def json_serialize(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

if __name__ == "__main__":
    pass