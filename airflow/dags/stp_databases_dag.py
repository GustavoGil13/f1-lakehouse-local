from datetime import datetime
from airflow import DAG
from controllers.airflow_services import spark_task
from conf.dag_config import STP_DATABASES_DAG_ID, STP_DATABASES_DAG_CONFIG


with DAG (
    dag_id = STP_DATABASES_DAG_ID
    , start_date = datetime(2022, 1, 1)
    , schedule = None
    , catchup = False
    , concurrency = 2
    , max_active_runs = 1
    , tags = ["openf1", "lakehouse", "stp", "databases"]
) as dag:

    # for each configured task
    for task_config in STP_DATABASES_DAG_CONFIG:
        # create task
        task = spark_task(task_config["task_id"], task_config["job_cmd"])
        # if dependencies list is empty then proceed
        if not task_config["dependencies"]:
            continue
        # establish the dependencies
        for dependency in task_config["dependencies"]:
            dag.get_task(dependency) >> task
