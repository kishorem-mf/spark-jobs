from datetime import datetime, timedelta

from airflow import DAG

from config import email_addresses
from custom_operators.databricks_functions import \
    DatabricksCreateClusterOperator, \
    DatabricksSubmitRunOperator, \
    DatabricksDeleteClusterOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 7),
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'pool': 'ohub-pool',
}

# az_deployment = '??/prod/deployment/'
# az_jar_bucket = f'{az_deployment}/ohub2.0'
# az_py_bucket = f'{az_deployment}/name-matching'
# az_data_input_bucket = '??/prod/data/raw/{}/**/*.csv'
# az_data_output_bucket = '??/prod/data/parquet/{}.parquet'
# jars = [f'{az_jar_bucket}/spark-jobs-assembly-0.1.jar']

cluster_name = 'ohub_basic'
databricks_conn_id = 'databricks_azure'

cluster_config = {
    'cluster_name': cluster_name,
    "spark_version": "3.5.x-scala2.11",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 16
}

with DAG('ohub_dag', default_args=default_args,
         schedule_interval="@once") as dag:
    create_cluster = DatabricksCreateClusterOperator(
        task_id='create_cluster',
        cluster_config=cluster_config,
        databricks_conn_id=databricks_conn_id,
        polling_period_seconds=10
    )

    delete_cluster = DatabricksDeleteClusterOperator(
        task_id='destroy_cluster',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id
    )

    # operators_to_csv = DatabricksSubmitRunOperator( )

    # operator_matching = DatabricksSubmitRunOperator( )

    # operator_merging = DatabricksSubmitRunOperator( )

    create_cluster >> delete_cluster
