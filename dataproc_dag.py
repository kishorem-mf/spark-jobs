from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import \
    DataprocClusterCreateOperator, DataprocClusterDeleteOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

cluster_defaults = {
    'gcp_conn_id': 'airflow-sp',
    'cluster_name': 'dummy',
    'project_id': 'ufs-prod',
    'region': 'europe-west4',
}

with DAG('dataproc_test', default_args=default_args,
         schedule_interval="@once") as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        num_workers=2,
        zone='europe-west4-c',
        **cluster_defaults)

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        **cluster_defaults)

create_cluster >> delete_cluster
