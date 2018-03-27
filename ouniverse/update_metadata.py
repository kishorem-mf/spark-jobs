
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from config import shared_default

default_args = {
    **shared_default,
}

dag = DAG('ouniverse_update_metadata', default_args=default_args,
          schedule_interval="0 0 1 * *")

remove_metadata_op = BashOperator(
    task_id="remove_metadata_operators",
    bash_command='echo "Remove metadata from operators"',
    dag=dag)

remove_metadata_grid = BashOperator(
    task_id="remove_metadata_grid",
    bash_command='echo "Remove metadata from gridsearch result"',
    dag=dag)

update_metadata_op = BashOperator(
    task_id="update_metadata_operators",
    bash_command='echo "Update metadata for operators"',
    dag=dag)

update_metadata_grid = BashOperator(
    task_id="update_metadata_grid",
    bash_command='echo "Update metadata for gridsearch"',
    dag=dag)

remove_metadata_op >> update_metadata_op
remove_metadata_grid >> update_metadata_grid
