from airflow import DAG
from datetime import datetime, timedelta

from config import country_codes, shared_default
from custom_operators.ga_fetch_operator import GAFetchOperator

default_args = {
    **shared_default,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG('gcp_ga', default_args=default_args) as dag:
    t1 = GAFetchOperator(
        task_id="Fetch Google Analytics from Google BigQuery for date",
        bigquery_conn_id='gcp_ga_conn_id',
        destination_folder="/tmp/ga/{{ds}}",
        date={'ds'},
        country_codes=country_codes)
