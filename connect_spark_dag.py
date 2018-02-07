from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator

from spark_job_config import ssh_hook, spark_cmd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('spark_connect_dag', default_args=default_args, schedule_interval="0 0 * * *")

t1 = SSHExecuteOperator(
    task_id="execute_bash_command",
    bash_command=spark_cmd(py_file='/notebook-dir/infra/name-matching/spark_ssh_test.py'),
    ssh_hook=ssh_hook,
    dag=dag)
