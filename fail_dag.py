from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['ufs-devs@googlegroups.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

with DAG('fail_test', default_args=default_args,
         schedule_interval="@once") as dag:
    create_cluster = BashOperator(
        task_id='fail_me',
        bash_command='return 1')
