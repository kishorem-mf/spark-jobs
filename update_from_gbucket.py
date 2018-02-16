from airflow import DAG
from datetime import datetime, timedelta

from custom_operators.dag_git_pull_operator import DagsGitPuller

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('update_dags', default_args=default_args,
          schedule_interval="0 * * * *")

t1 = DagsGitPuller(task_id="perform_git_pull", dag=dag)
