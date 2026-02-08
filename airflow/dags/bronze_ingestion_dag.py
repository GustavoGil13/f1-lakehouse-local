from datetime import datetime
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

DAG_ID = "openf1_bronze_ingestion"

def spark_task(task_id: str, job_cmd: str) -> DockerOperator:
    return DockerOperator (
        task_id = task_id
        , image = os.environ.get("SPARK_IMAGE")
        , api_version = "auto"
        , docker_url = "unix://var/run/docker.sock"
        , network_mode = os.environ.get("DOCKER_NETWORK")
        , auto_remove = True
        , environment = {
            "S3A_ACCESS_KEY": os.environ.get("S3A_ACCESS_KEY")
            , "S3A_SECRET_KEY": os.environ.get("S3A_SECRET_KEY")
            , "S3A_ENDPOINT": os.environ.get("S3A_ENDPOINT")
            , "AWS_ACCESS_KEY_ID": os.environ.get("S3A_ACCESS_KEY")
            , "AWS_SECRET_ACCESS_KEY": os.environ.get("S3A_SECRET_KEY")
            , "BRONZE_DELTA_PATH": os.environ.get("BRONZE_DELTA_PATH")
            , "SILVER_DELTA_PATH": os.environ.get("SILVER_DELTA_PATH")
            , "DQ_DELTA_PATH": os.environ.get("DQ_DELTA_PATH")
        }
        , command = [
            "bash"
            , "-lc"
            , f"""/opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
              --conf spark.ui.showConsoleProgress=false \
              {job_cmd}
            """
        ]
        , mount_tmp_dir = False,
    )

with DAG (
    dag_id = DAG_ID
    , start_date = datetime(2022, 1, 1)
    , schedule = None
    , catchup = False
    , tags = ["openf1", "lakehouse", "bronze"]
) as dag:

    year = "{{ ds[:4] }}"

    meetings = spark_task (
        "meetings"
        , f"/opt/spark/jobs/bronze/bronze_ingestion_by_year.py --endpoint meetings --year {year}"
    )

    sessions = spark_task (
        "sessions"
        , f"/opt/spark/jobs/bronze/bronze_ingestion_by_key.py --source_endpoint meetings --year {year} --key meeting_key --target_endpoint sessions"
    )

    drivers = spark_task (
        "drivers"
        , f"/opt/spark/jobs/bronze/bronze_ingestion_by_key.py --source_endpoint meetings --year {year} --key meeting_key --target_endpoint drivers"
    )

    session_result = spark_task (
        "session_result"
        , f"/opt/spark/jobs/bronze/bronze_ingestion_by_key.py --source_endpoint meetings --year {year} --key meeting_key --target_endpoint session_result"
    )

    laps = spark_task (
        "laps"
        , f"/opt/spark/jobs/bronze/bronze_ingestion_by_key.py --source_endpoint sessions --year {year} --key session_key --target_endpoint laps --target_endpoint_filter_column session_type --target_endpoint_filter_operator ne --target_endpoint_filter_value Practice"
    )

    meetings >> [sessions, drivers, session_result]
    sessions >> laps
