from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

with DAG(
    dag_id=f"training_pipeline_v1_0",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    hypertune_model = BashOperator(
        task_id="hypertune_model",
        bash_command=f"python -m model hypertune_model --base_path {AIRFLOW_HOME}",
    )

    training_model = BashOperator(
        task_id="training_model",
        bash_command=f"python -m model training_model --base_path {AIRFLOW_HOME}",
    )

    hypertune_model >> training_model
