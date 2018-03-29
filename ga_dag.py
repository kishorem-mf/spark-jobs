from airflow import DAG
from datetime import datetime, timedelta

from config import email_addresses, country_codes
from custom_operators.ga_fetch_operator import GAToGSOperator, LocalGAToWasbOperator, GSToLocalOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2018, 3, 28),
}

local_path = '/tmp/gs_export/'
remote_bucket = 'digitaldataufs'
path_in_bucket = '/ga_data'

with DAG('gcp_ga', default_args=default_args, schedule_interval='@once') as dag:
    ga_to_gs = GAToGSOperator(
        task_id="fetch_GA_from_BQ_for_date",
        bigquery_conn_id='gcp_storage',
        destination='gs://' + remote_bucket + path_in_bucket,
        date='{{ ds }}',
        country_codes=country_codes)

    gs_to_local = GSToLocalOperator(
        task_id='gcp_bucket_to_local',
        path=local_path + '{{gs}}',
        date='{{ds}}',
        bucket=remote_bucket,
        path_in_bucket=path_in_bucket,
        gcp_conn_id='gcp_storage',
        country_codes=country_codes
    )

    local_to_wasb = LocalGAToWasbOperator(
        task_id='local_to_azure',
        wasb_conn_id='azure_blob',
        path=local_path + '{{ds}}/',
        date='{{ds}}',
        country_codes=country_codes,
        container_name='prod',
        blob_path='data/raw/gaData/'
    )

    ga_to_gs >> gs_to_local >> local_to_wasb
