from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG(dag_id='hi', default_args={'start_date': datetime(2016, 1, 1)}) as dag:
    a = BashOperator(task_id='a', bash_command='echo a')
    b = BashOperator(task_id='b', bash_command='echo b')
    c = BashOperator(task_id='c', bash_command='echo c')
    d = BashOperator(task_id='d', bash_command='echo d')
    e = BashOperator(task_id='e', bash_command='echo e')
    a >> c >> d
    b >> c >> e
