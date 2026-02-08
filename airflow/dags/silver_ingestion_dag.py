from datetime import datetime
from airflow import DAG
from controllers.airflow_services import spark_task
from conf.dag_config import SILVER_DAG_ID, SILVER_DAG_CONFIG

with DAG (
    dag_id = SILVER_DAG_ID
    , start_date = datetime(2022, 1, 1)
    , schedule = None
    , catchup = False
    , concurrency = 2
    , tags = ["openf1", "lakehouse", "silver"]
) as dag:

    # get year from ds
    year = "{{ ds[:4] }}"
    # for each configured task
    for task_config in SILVER_DAG_CONFIG:
        # create task
        task = spark_task(task_config["task_id"], f"/opt/spark/jobs/silver/{task_config["task_id"]}.py --year {year}")
        # if has dq tests then create validation task
        if task_config["has_dq"]:
            task >> spark_task(f"{task_config["task_id"]}_validation", f"/opt/spark/jobs/tests/dq_runner.py --table {task_config["task_id"]} --year {year}")
        # if dependencies list is empty then proceed
        if not task_config["dependencies"]:
            continue
        # establish the dependencies
        for dependency in task_config["dependencies"]:
            dag.get_task(dependency) >> task
