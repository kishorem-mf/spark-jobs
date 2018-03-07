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
    'start_date': datetime.now(),
}

templeted_destination_folder = "/tmp/ga/{{ds}}"

with DAG('gcp_ga', default_args=default_args) as dag:
    t1 = GAFetchOperator(
        task_id="Fetch Google Analytics from Google BigQuery for date",
        bigquery_conn_id='gcp_ga_conn_id',
        destination_folder=templeted_destination_folder,
        date={'ds'},
        country_codes=country_codes)
