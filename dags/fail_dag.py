from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from config import email_addresses, slack_on_databricks_failure_callback
from dags.custom_operators.databricks_functions import DatabricksSubmitRunOperator
from operators_config import cluster_id, databricks_conn_id

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': email_addresses,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack_on_databricks_failure_callback
}

with DAG('fail_test', default_args=default_args,
         schedule_interval="@once") as dag:
    # create_cluster = BashOperator(
    #     task_id='fail_me',
    #     bash_command='return 1')
    databricks_fail = DatabricksSubmitRunOperator(
        task_id="fail_databricks",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'non_existing_jar'}
        ],
        spark_jar_task={
            'main_class_name': "foo",
        }
    )
