from airflow import DAG
from datetime import datetime, timedelta

from airflow import settings
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG('update_dags', default_args=default_args,
         schedule_interval="0 * * * *") as dag:

    t1 = GoogleCloudStorageDownloadOperator(
        bucket='gs://ufs-prod/airflow-dags/',
        object='dags.tar',
        filename=f'{settings.DAGS_FOLDER}/dags.tar',
        google_cloud_storage_conn_id='',
        task_id="download_dag_tar")
    t2 = BashOperator(
        bash_command=f'tar -xf {settings.DAGS_FOLDER}/dags.tar {settings.DAGS_FOLDER}/'
    )

    t1 >> t2
