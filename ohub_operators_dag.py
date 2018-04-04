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
    'start_date': datetime(2018, 3, 7),
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

wasb_root_bucket = 'wasbs://prod@ulohub2storedevne.blob.core.windows.net/data/'
raw_bucket = wasb_root_bucket + 'raw/{date}/{schema}/**/*.csv'
ingested_bucket = wasb_root_bucket + 'ingested/{date}/{fn}.parquet'
intermediate_bucket = wasb_root_bucket + 'intermediate/{date}/{fn}.parquet'
integrated_bucket = wasb_root_bucket + 'integrated/{date}/{fn}.parquet'
export_bucket = wasb_root_bucket + 'export/{date}/{fn}.parquet'

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
            'parameters': [raw_bucket.format(date='2017-07-12', schema='operators'),
                           ingested_bucket.format(date='2017-07-12', fn='operators')]
        }
    )

    deduplicate_daily = DatabricksSubmitRunOperator(
        task_id="deduplicate_daily",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.deduplicate.OperatorDeduplication",
            'parameters': [integrated_bucket.format(date='2017-07-12', fn='operators'),
                           ingested_bucket.format(date='2017-07-12', fn='operators'),
                           intermediate_bucket.format(date='2018-04-04', fn='operators_integrated'),
                           intermediate_bucket.format(date='2018-04-04', fn='operators_daily')]
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
            'parameters': ['--input_file', intermediate_bucket.format(date='2017-07-12', fn='operators'),
                           '--output_path', intermediate_bucket.format(date='2017-07-12', fn='operators_matched'),
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
            'parameters': [intermediate_bucket.format(date='2017-07-12', fn='operators_matched'),
                           ingested_bucket.format(date='2017-07-12', fn='operators'),
                           integrated_bucket.format(date='2017-07-12', fn='operators_merged')]
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
            'parameters': [integrated_bucket.format(date='2017-07-12', fn='operators_merged.parquet'),
                           export_bucket.format(date='2017-07-12', fn='acm/operators.csv'),
                           postgres_connection.host,
                           postgres_connection.login,
                           postgres_connection.password,
                           postgres_connection.schema]
        }
    )

    start_cluster >> operators_to_parquet >> deduplicate_daily >> match_operators >> merge_operators >> \
    operators_to_acm >> terminate_cluster
