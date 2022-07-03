from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

with DAG(
    dag_id=f"data_pipeline_v1_0",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    validate_assets_task = BashOperator(
        task_id="validate_assets",
        bash_command=f"python -m model validate_assets --base_path {AIRFLOW_HOME}",
    )

    preprocessing_task = BashOperator(
        task_id="preprocessing",
        bash_command=f"python -m model preprocess_assets --base_path {AIRFLOW_HOME}",
    )

    feature_engineering_task = BashOperator(
        task_id=f"feature_engineering",
        bash_command=f"python -m model feature_engineering --base_path {AIRFLOW_HOME}",
    )

    validate_assets_task >> preprocessing_task >> feature_engineering_task
