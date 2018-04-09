from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook

from config import email_addresses
from custom_operators.databricks_functions import \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator, \
    DatabricksStartClusterOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 7, 12),
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dbfs_root_bucket = 'dbfs:/mnt/ohub_data/'
raw_bucket = dbfs_root_bucket + 'raw/{schema}/{date}/**/*.csv'
ingested_bucket = dbfs_root_bucket + 'ingested/{date}/{fn}.parquet'
intermediate_bucket = dbfs_root_bucket + 'intermediate/{date}/{fn}.parquet'
integrated_bucket = dbfs_root_bucket + 'integrated/{date}/{fn}.parquet'
export_bucket = dbfs_root_bucket + 'export/{date}/{fn}.parquet'

cluster_id = '0314-131901-shalt605'
databricks_conn_id = 'databricks_azure'

jar = 'dbfs:/libraries/ohub/spark-jobs-assembly-0.2.0.jar'

with DAG('ohub_operators_first_ingest', default_args=default_args,
         schedule_interval="@once") as dag:
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

    operators_to_parquet = DatabricksSubmitRunOperator(
        task_id="operators_to_parquet",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.tsv2parquet.file_interface.OperatorConverter",
            'parameters': [raw_bucket.format(date='{{ds}}', schema='operators'),
                           ingested_bucket.format(date='{{ds}}', fn='operators')]
        }
    )

    match_operators = DatabricksSubmitRunOperator(
        task_id='match_unmatched_operators',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'egg': 'dbfs:/libraries/name_matching/string_matching.egg'}
        ],
        spark_python_task={
            'python_file': 'dbfs:/libraries/name_matching/match_operators.py',
            'parameters': ['--input_file', ingested_bucket.format(date='{{ds}}', fn='operators'),
                           '--output_path', intermediate_bucket.format(date='{{ds}}', fn='operators_matched'),
                           '--country_code', 'DK']
        }
    )

    merge_operators = DatabricksSubmitRunOperator(
        task_id='merge_operators',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMerging",
            'parameters': [intermediate_bucket.format(date='{{ds}}', fn='operators_matched'),
                           ingested_bucket.format(date='{{ds}}', fn='operators'),
                           integrated_bucket.format(date='{{ds}}', fn='operators')]
        })

    postgres_connection = BaseHook.get_connection('postgres_channels')
    operators_to_acm = DatabricksSubmitRunOperator(
        task_id="operators_to_acm",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.OperatorAcmConverter",
            'parameters': [integrated_bucket.format(date='{{ds}}', fn='operators'),
                           export_bucket.format(date='{{ds}}', fn='acm/operators.csv'),
                           postgres_connection.host,
                           postgres_connection.login,
                           postgres_connection.password,
                           postgres_connection.schema]
        }
    )

    update_operators_table = DatabricksSubmitRunOperator(
        task_id='update_operators_table',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        notebook_task={'notebook_path': '/Users/tim.vancann@unilever.com/update_integrated_tables'}
    )

    start_cluster >> operators_to_parquet >> match_operators >> merge_operators >> operators_to_acm >> terminate_cluster
