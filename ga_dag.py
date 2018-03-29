from pprint import pprint

from airflow import DAG
from datetime import datetime, timedelta

from airflow.contrib.operators.file_to_wasb import FileToWasbOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.operators.python_operator import PythonOperator

from config import email_addresses
from custom_scripts import ga_to_gs

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime.now(),
}

with DAG('gcp_ga', default_args=default_args) as dag:
    ga_to_gs = PythonOperator(
        task_id='ga_to_gs',
        python_callable=ga_to_gs.main,
        provide_context=True)
    gs_to_local = GoogleCloudStorageDownloadOperator(
        task_id='gs_to_local',
        google_cloud_storage_conn_id='gcp_storage',
        bucket='ufs-accept',
        object='ga_data/PARTITION_DATE={{ds}}/',
        filename='/root/gs_export/{{ds}}/'

    )
    local_to_azure = FileToWasbOperator(
        task_id='local_to_azure',
        wasb_conn_id='azure_blob',

    )

    ga_to_gs >> gs_to_local >> local_to_azure
