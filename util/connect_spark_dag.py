from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks import SSHHook

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

spark_cmd = """sudo su && \
$SPARK_HOME/bin/spark-submit /notebook-dir/infra/name-matching/spark_ssh_test.py
"""

test_cmd = """echo $SPARK_HOME"""
ssh_hook = SSHHook(conn_id='spark_edgenode_ssh')

t1 = SSHExecuteOperator(
    task_id="execute_bash_command",
    bash_command=spark_cmd,
    ssh_hook=ssh_hook,
    dag=dag)
