from datetime import datetime
from airflow import DAG
from controllers.airflow_services import spark_task, trigger_dag
from conf.dag_config import BRONZE_DAG_ID, BRONZE_DAG_CONFIG, SILVER_DAG_ID


with DAG (
    dag_id = BRONZE_DAG_ID
    , start_date = datetime(2022, 1, 1)
    , schedule = None
    , catchup = False
    , concurrency = 2
    , tags = ["openf1", "lakehouse", "bronze"]
) as dag:

    # get year from ds
    year = "{{ ds[:4] }}"
    # for each configured task
    for task_config in BRONZE_DAG_CONFIG:
        # create task
        task = spark_task(task_config["task_id"], task_config["job_cmd"].format(year = year))
        # if dependencies list is empty then proceed
        if not task_config["dependencies"]:
            continue
        # establish the dependencies
        for dependency in task_config["dependencies"]:
            dag.get_task(dependency) >> task
    # create trigger for silver dag
    silver_trigger = trigger_dag(f"trigger_{SILVER_DAG_ID}", SILVER_DAG_ID)
    # establish the dependencies between leaves and the silver trigger task
    leaf_tasks = [t for t in dag.leaves if t.task_id != f"trigger_{SILVER_DAG_ID}"]
    leaf_tasks >> silver_trigger
