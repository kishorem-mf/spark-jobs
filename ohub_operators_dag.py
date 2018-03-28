from datetime import datetime, timedelta

from airflow import DAG

from config import email_addresses
from custom_operators.databricks_functions import \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator, \
    DatabricksStartClusterOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 7),
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
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
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id
    )

    operators_to_parquet = DatabricksSubmitRunOperator(
        task_id="operators_to_parquet",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.tsv2parquet.OperatorConverter",
            'parameters': [data_input_bucket.format('OPERATORS'),
                           data_output_bucket.format('OPERATORS'.lower())]
        }
    )

    match_operators = DatabricksSubmitRunOperator(
        task_id='match_operators',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'egg': 'dbfs:/libraries/string_matching.egg'}
        ],
        spark_python_task={
            'python_file': 'dbfs:/libraries/match_operators.py',
            'parameters': ['--input_file', data_output_bucket.format('operators'),
                           '--output_path', data_output_bucket.format('operators_matched')]
        }
    )

    merge_operators = DatabricksSubmitRunOperator(
        task_id='merge_operators',
        cluster_name=cluster_name,
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
        cluster_name=cluster_name,
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

    start_cluster >> operators_to_parquet >> match_operators >> merge_operators >> operators_to_acm >> terminate_cluster
