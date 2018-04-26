from airflow import DAG
from datetime import datetime, timedelta

from config import email_addresses, country_codes, slack_on_databricks_failure_callback
from custom_operators.databricks_functions import DatabricksSubmitRunOperator, DatabricksStartClusterOperator, \
    DatabricksTerminateClusterOperator
from custom_operators.ga_fetch_operator import GAToGSOperator, LocalGAToWasbOperator, GSToLocalOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2018, 3, 26),
    'on_failure_callback': slack_on_databricks_failure_callback
}

local_path = '/tmp/gs_export/'
remote_bucket = 'digitaldataufs'
path_in_bucket = 'ga_data'

cluster_id = '0405-082501-flare296'
databricks_conn_id = 'databricks_azure'

with DAG('gcp_ga', default_args=default_args, schedule_interval='0 4 * * *') as dag:
    ga_to_gs = GAToGSOperator(
        task_id="fetch_GA_from_BQ_for_date",
        bigquery_conn_id='gcp_storage',
        destination='gs://' + remote_bucket + '/' + path_in_bucket,
        date='{{ macros.ds_add(ds, -1) }}',
        country_codes=country_codes)

    gs_to_local = GSToLocalOperator(
        task_id='gcp_bucket_to_local',
        path=local_path,
        date='{{ macros.ds_add(ds, -1) }}',
        bucket=remote_bucket,
        path_in_bucket=path_in_bucket,
        gcp_conn_id='gcp_storage',
        country_codes=country_codes
    )

    local_to_wasb = LocalGAToWasbOperator(
        task_id='local_to_azure',
        wasb_conn_id='azure_blob',
        path=local_path,
        date='{{ macros.ds_add(ds, -1) }}',
        country_codes=country_codes,
        container_name='prod',
        blob_path='data/raw/gaData/'
    )

    start_cluster = DatabricksStartClusterOperator(
        task_id='start_cluster',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id
    )

    terminate_cluster = DatabricksTerminateClusterOperator(
        task_id='terminate_cluster',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id
    )

    update_ga_table = DatabricksSubmitRunOperator(
        task_id='update_ga_table',
        existing_cluster_id='0405-082501-flare296',
        databricks_conn_id=databricks_conn_id,
        notebook_task={'notebook_path': '/Users/tim.vancann@unilever.com/update_ga_tables'}
    )

    ga_to_gs >> gs_to_local >> local_to_wasb >> update_ga_table
    start_cluster >> update_ga_table >> terminate_cluster
