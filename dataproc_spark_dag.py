from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('spark_connect_dag', default_args=default_args,
         schedule_interval="@once") as dag:

    create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        cluster_name='dummy',
        project_id='ufs-prod',
        num_workers=2,
        region='europe-west4',
        zone='europe-west4-c',
        gcp_conn_id='airflow-sp')

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        cluster_name='dummy',
        project_id='ufs-prod',
        region='europe-west4',
        gcp_conn_id='airflow-sp')
