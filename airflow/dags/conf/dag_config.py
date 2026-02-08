BRONZE_DAG_ID = "openf1_bronze_ingestion"

BRONZE_DAG_CONFIG = [
    {"task_id": "meetings", "job_cmd": "/opt/spark/jobs/bronze/bronze_ingestion_by_year.py --endpoint meetings --year {year}", "dependencies": []}
    , {"task_id": "sessions", "job_cmd": "/opt/spark/jobs/bronze/bronze_ingestion_by_key.py --source_endpoint meetings --year {year} --key meeting_key --target_endpoint sessions", "dependencies": ["meetings"]}
    , {"task_id": "drivers", "job_cmd": "/opt/spark/jobs/bronze/bronze_ingestion_by_key.py --source_endpoint meetings --year {year} --key meeting_key --target_endpoint drivers", "dependencies": ["meetings"]}
    , {"task_id": "session_result", "job_cmd": "/opt/spark/jobs/bronze/bronze_ingestion_by_key.py --source_endpoint meetings --year {year} --key meeting_key --target_endpoint session_result", "dependencies": ["meetings"]}
    , {"task_id": "laps", "job_cmd": "/opt/spark/jobs/bronze/bronze_ingestion_by_key.py --source_endpoint sessions --year {year} --key session_key --target_endpoint laps --target_endpoint_filter_column session_type --target_endpoint_filter_operator ne --target_endpoint_filter_value Practice", "dependencies": ["sessions"]}
]

SILVER_DAG_ID = "openf1_silver_ingestion"

SILVER_DAG_CONFIG = [
    {"task_id": "circuits", "has_dq": True, "dependencies": []}
    , {"task_id": "countries", "has_dq": True, "dependencies": []}
    , {"task_id": "locations", "has_dq": True, "dependencies": []}
    , {"task_id": "meetings", "has_dq": True, "dependencies": []}
    , {"task_id": "sessions", "has_dq": True, "dependencies": []}
    , {"task_id": "teams", "has_dq": True, "dependencies": []}
    , {"task_id": "drivers", "has_dq": True, "dependencies": []}
    , {"task_id": "drivers_sessions_association", "has_dq": True, "dependencies": []}
    , {"task_id": "session_result", "has_dq": True, "dependencies": []}
    , {"task_id": "laps", "has_dq": True, "dependencies": []}
]
