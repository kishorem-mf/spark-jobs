from airflow import DAG
from datetime import datetime, timedelta

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

dag = DAG('gcp_ga', default_args=default_args)

t1 = GAFetchOperator(task_id="perform_ga_fetch",
                     dag=dag,
                     bigquery_conn_id='gcp_ga_conn_id',
                     country_codes={'ga_country_codes'},
                     destination_folder={'ga_destination_folder'},
                     date_from={'ga_from_date'},
                     date_to={'ga_to_date'}),
