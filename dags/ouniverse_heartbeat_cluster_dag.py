from datetime import datetime, timedelta

from airflow import DAG

from config import email_addresses
from custom_operators.databricks_functions import \
    DatabricksTerminateClusterOperator, \
    DatabricksStartClusterOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': datetime(2018, 4, 13),
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

cluster_id = '0412-111727-sock927'
databricks_conn_id = 'databricks_azure'


with DAG('ouniverse_heartbeat_cluster', default_args=default_args,
         schedule_interval="0 0 * * *") as dag:
    start_cluster = DatabricksStartClusterOperator(
        task_id='start_cluster',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id
    )

    terminate_cluster = DatabricksTerminateClusterOperator(
        task_id='terminate_cluster',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id
    )

    start_cluster >> terminate_cluster
