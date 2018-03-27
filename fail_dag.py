from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from config import shared_default

default_args = {
    **shared_default,
}

with DAG('fail_test', default_args=default_args,
         schedule_interval="@once") as dag:
    create_cluster = BashOperator(
        task_id='fail_me',
        bash_command='return 1')
