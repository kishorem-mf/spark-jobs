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
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

wasb_root_bucket = 'wasbs://prod@ulohub2storedevne.blob.core.windows.net/data/'
raw_bucket = wasb_root_bucket + 'raw/{}/**/*.csv'
intermediate_bucket = wasb_root_bucket + 'intermediate/{}.parquet'
integrated_bucket = wasb_root_bucket + 'integrated/{}.parquet'
export_bucket = wasb_root_bucket + 'export/{}.parquet'

cluster_name = 'ohub_basic'
cluster_id = '0314-131901-shalt605'

databricks_conn_id = 'databricks_azure'

cluster_config = {
    'cluster_name': cluster_name,
    "spark_version": "3.5.x-scala2.11",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 16
}

jar = 'dbfs:/libraries/ohub/spark-jobs-assembly-WIP.jar'

with DAG('ohub_operators', default_args=default_args,
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
            'parameters': [raw_bucket.format('OPERATORS'),
                           intermediate_bucket.format('operators')]
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
            'parameters': ['--input_file', intermediate_bucket.format('operators'),
                           '--output_path', intermediate_bucket.format('operators_matched'),
                           '--country_code', 'DK']
        }
    )

    # persistent_uuid = DatabricksSubmitRunOperator(
    #     task_id='persistent_uuid',
    #     cluster_name=cluster_name,
    #     databricks_conn_id=databricks_conn_id,
    #     libraries=[
    #         {'egg': 'dbfs:/libraries/string_matching.egg'}
    #     ],
    #     spark_python_task={
    #         'python_file': 'dbfs:/libraries/join_new_operators_with_persistent_uuid.py',
    #         'parameters': [
    #             '--current_operators_path', data_output_bucket.format('OPERATORS'),
    #             '--new_operators_path', data_output_bucket.format('operators_matched'),
    #             '--output_path', data_output_bucket.format('operators_uuid'),
    #             '--country_code', 'all',
    #             '--threshold', '0.8',
    #         ]
    #     }
    # )

    merge_operators = DatabricksSubmitRunOperator(
        task_id='merge_operators',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMerging",
            'parameters': [intermediate_bucket.format('operators_matched'),
                           intermediate_bucket.format('operators'),
                           integrated_bucket.format('operators_merged')]
        })

    operators_to_acm = DatabricksSubmitRunOperator(
        task_id="operators_to_acm",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.OperatorAcmConverter",
            'parameters': [integrated_bucket.format('operators_merged.parquet'),
                           export_bucket.format('acm/operators.csv')]
        }
    )

    start_cluster >> operators_to_parquet >> match_operators >> merge_operators >> operators_to_acm >> terminate_cluster
