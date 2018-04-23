from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from config import email_addresses, slack_on_failure_callback

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': email_addresses,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

with DAG('fail_test', default_args=default_args,
         schedule_interval="@once") as dag:
    create_cluster = BashOperator(
        task_id='fail_me',
        bash_command='return 1',
        on_failure_callback=slack_on_failure_callback)
