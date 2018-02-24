from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('new_leads', default_args=default_args,
         schedule_interval='0 0 1 * *') as dag:
    phase_one = BashOperator(
        task_id="phase_one",
        bash_command='echo "execute ouniverse phase I"')

    phase_two_grid = BashOperator(
        task_id="phase_two_grid",
        bash_command='echo "execute ouniverse phase II grid search"')

    phase_two_ids = BashOperator(
        task_id="phase_two_ids",
        bash_command='echo "execute ouniverse phase II id metadata"')

    create_dataproc = BashOperator(
        task_id="prioritise_leads",
        bash_command='echo "Start dataproc cluster"')

    delete_dataproc = BashOperator(
        task_id="prioritise_leads",
        bash_command='echo "remove dataproc cluster"')

    prioritize = BashOperator(
        task_id="prioritise_leads",
        bash_command='echo "execute spark job"')

phase_one >> create_dataproc
phase_two_grid >> phase_two_ids >> create_dataproc
create_dataproc >> prioritize >> delete_dataproc
