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
    'start_date': datetime.now().__format__('YYYYMMDD'),
    'end_date': datetime.now().__format__('YYYYMMDD'),
    'bigquery_conn_id': 'bigquery_default',
    'destination_folder': 'gs://digitaldataufs/ga_dump',
    'country_codes': [
        'AU',
        'NZ',
        'BE',
        'FR',
        'NL',
        # CN,
        'AT',
        'DE',
        'CH',
        # IN,
        'IL',
        'GR',
        'IT',
        'MQ',
        'LK',
        'PK',
        'SA',
        'HK',
        'TW',
        # KR,
        'CA',
        'US',
        'CZ',
        'SK',
        'EE',
        'PL',
        'CO',
        'MX',
        # LA,
        # DK,
        'FI',
        'NO',
        # SE,
        'PT',
        'RU',
        'ZA',
        'ID',
        'MY',
        # PH,
        'SG',
        'TH',
        'VN',
        'BG',
        'HU',
        'RO',
        'AR',
        'BR',
        'CL',
        'ES',
        'TR',
        'IE',
        'GB',
    ],
}

dag = DAG('ga_dags', default_args=default_args)

t1 = GAFetchOperator(task_id="perform_ga_fetch",
                    dag=dag,
                    bigquery_conn_id=default_args.get('bigquery_conn_id'),
                    country_codes=default_args.get('coutry_codes'),
                    start_date=default_args.get('start_date'),
                    end_date=default_args.get('end_date'),
                    destination_folder=default_args.get('destination_folder'))
