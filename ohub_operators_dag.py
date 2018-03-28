from datetime import datetime, timedelta

from airflow import DAG

from config import shared_default
from custom_operators.databricks_functions import \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator, \
    DatabricksStartClusterOperator

default_args = {
    **shared_default,
    'retries': 0,
}

wasb_root_bucket = 'wasbs://prod@ulohub2storedevne.blob.core.windows.net/data/'
data_input_bucket = wasb_root_bucket + 'raw/{}/**/*.csv'
data_output_bucket = wasb_root_bucket + 'parquet/{}.parquet'

cluster_name = 'ohub_basic'
cluster_id = '0314-131901-shalt605'

databricks_conn_id = 'databricks_azure'

cluster_config = {
    'cluster_name': cluster_name,
    "spark_version": "3.5.x-scala2.11",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 16
}

with DAG('ohub_operators', default_args=default_args,
         schedule_interval="@once") as dag:
    start_cluster = DatabricksStartClusterOperator(
        task_id='start_cluster',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        polling_period_seconds=10
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
            {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.tsv2parquet.OperatorConverter",
            'parameters': [data_input_bucket.format('OPERATORS'),
                           data_output_bucket.format('operators')]
        }
    )

    match_operators = DatabricksSubmitRunOperator(
        task_id='match_operators',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'egg': 'dbfs:/libraries/name_matching/string_matching.egg'}
        ],
        spark_python_task={
            'python_file': 'dbfs:/libraries/name_matching/match_operators.py',
            'parameters': ['--input_file', data_output_bucket.format('operators'),
                           '--output_path', data_output_bucket.format('operators_matched')]
        }
    )

    uuid_operators = DatabricksSubmitRunOperator(
        task_id='uuid_operators',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'egg': 'dbfs:/libraries/string_matching.egg'}
        ],
        spark_python_task={
            'python_file': 'dbfs:/libraries/join_new_operators_with_persistent_uuid.py',
            'parameters': [
                '--current_operators_path', data_output_bucket.format('current_operators'),
                '--new_operators_path', data_output_bucket.format('new_operators'),
                '--output_path', data_output_bucket.format('deduped'),
                '--country_code', 'all',
                '--threshold', '0.8',
            ]
        }
    )

    merge_operators = DatabricksSubmitRunOperator(
        task_id='merge_operators',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMerging",
            'parameters': [data_output_bucket.format('operators_matched'),
                           data_input_bucket.format('OPERATORS'),
                           data_output_bucket.format('operators_merged')]
        })

    operators_to_acm = DatabricksSubmitRunOperator(
        task_id="operators_to_acm",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.tsv2parquet.OperatorAcmConverter",
            'parameters': [data_output_bucket.format('operators_merged.parquet'),
                           data_output_bucket.format('operators_acm.csv')]
        }
    )

    start_cluster >> operators_to_parquet >> match_operators >> uuid_operators \
        >> merge_operators >> operators_to_acm >> terminate_cluster
