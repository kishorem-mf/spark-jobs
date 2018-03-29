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
    'start_date': datetime.now(),
}

local_path = '/tmp/gs_export'

with DAG('gcp_ga', default_args=default_args) as dag:
    ga_to_gs = GAToGSOperator(
        task_id="fetch_GA_from_BQ_for_date",
        bigquery_conn_id='gcp_ga_conn_id',
        destination_folder=local_path + '/{{ds}}',
        date='{{ ds }}',
        country_codes=country_codes)

    local_to_wasb = LocalGAToWasbOperator(
        task_id='local_to_azure',
        wasb_conn_id='azure_blob',
        path=local_path + '{{ds}}/',
        container_name='prod',
        blob_path='data/raw/gaData/'
    )

    ga_to_gs >> local_to_wasb
