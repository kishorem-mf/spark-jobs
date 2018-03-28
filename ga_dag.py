from airflow import DAG
from datetime import datetime, timedelta

from config import country_codes
from custom_operators.ga_fetch_operator import GAFetchOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2018, 3, 28),
}

with DAG('gcp_ga', default_args=default_args) as dag:
    t1 = GAFetchOperator(
        task_id="fetch_GA_from_BQ_for_date",
        bigquery_conn_id='gcp_ga_conn_id',
        destination_folder="/tmp/ga/{{ds}}",
        date='{{ ds }}',
        country_codes=country_codes)
