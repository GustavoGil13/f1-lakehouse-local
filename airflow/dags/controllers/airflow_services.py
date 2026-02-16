import os
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def spark_task(task_id: str, job_cmd: str) -> DockerOperator:
    return DockerOperator (
        task_id = task_id
        , image = "f1-spark:local"
        , api_version = "auto"
        , docker_url = "unix://var/run/docker.sock"
        , network_mode = os.environ.get("DOCKER_NETWORK")
        , auto_remove = True
        , environment = {
            "S3A_ACCESS_KEY": os.environ.get("S3A_ACCESS_KEY")
            , "S3A_SECRET_KEY": os.environ.get("S3A_SECRET_KEY")
            , "S3A_ENDPOINT": os.environ.get("S3A_ENDPOINT")
            , "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID")
            , "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY")
            , "HIVE_WAREHOUSE_DIR": os.environ.get("HIVE_WAREHOUSE_DIR")
            , "BRONZE_DB": os.environ.get("BRONZE_DB")
            , "SILVER_DB": os.environ.get("SILVER_DB")
            , "DQ_DB": os.environ.get("DQ_DB")
        }
        , command = [
            "bash"
            , "-lc"
            , f"""/opt/spark/bin/spark-submit \
              --master spark://spark-master:7077 \
              --conf spark.hadoop.fs.s3a.endpoint=$S3A_ENDPOINT \
              {job_cmd}
            """
        ]
        , mount_tmp_dir = False,
    )


def trigger_dag(task_id: str, dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator (
        task_id = task_id
        , trigger_dag_id = dag_id
        , reset_dag_run = True
        , wait_for_completion = False
        , execution_date = "{{ ds }}"
    )
    

if __name__ == "__main__":
    # Module intended for import; no CLI behavior.
    pass
