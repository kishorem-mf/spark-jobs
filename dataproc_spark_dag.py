from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

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

with DAG('spark_connect_dag', default_args=default_args,
         schedule_interval="0 0 * * *") as dag:

    create_dataproc = DataprocClusterCreateOperator(
        cluster_name='dummy',
        project_id='ufs-prod',
        num_workers=2,
        zone='europe-west4-c',
        gcp_conn_id='',
        service_account='airflow@ufs-prod.iam.gserviceaccount.com')
