from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook

from config import email_addresses, operator_country_codes
from custom_operators.databricks_functions import \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator, \
    DatabricksStartClusterOperator, \
    DatabricksUninstallLibrariesOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 7),
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'pool': 'ohub_pool'
}

dbfs_root_bucket = 'dbfs:/mnt/ohub_data/'
raw_bucket = dbfs_root_bucket + 'raw/{schema}/{date}/**/*.csv'
ingested_bucket = dbfs_root_bucket + 'ingested/{date}/{fn}.parquet'
intermediate_bucket = dbfs_root_bucket + 'intermediate/{date}/{fn}.parquet'
integrated_bucket = dbfs_root_bucket + 'integrated/{date}/{fn}.parquet'
export_bucket = dbfs_root_bucket + 'export/{date}/{fn}.parquet'

cluster_id = '0314-131901-shalt605'
databricks_conn_id = 'databricks_azure'

one_day_ago = '2018-04-06'  # should become {{ ds }}
two_day_ago = '2017-07-12'  # should become {{ macros.ds_add(ds, -1) }}

jar = 'dbfs:/libraries/ohub/spark-jobs-assembly-0.2.0.jar'
egg = 'dbfs:/libraries/name_matching/string_matching.egg'

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

    uninstall_old_libraries = DatabricksUninstallLibrariesOperator(
        task_id='uninstall_old_libraries',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries_to_uninstall=[{
            'jar': jar,
            'egg': egg,
        }]
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
            'parameters': ['--input_file', raw_bucket.format(date=one_day_ago, schema='operators'),
                           '--output_path', ingested_bucket.format(date=one_day_ago, fn='operators'),
                           '--strict_ingestion', "false"]
        }
    )

    tasks = []
    for code in operator_country_codes:
        tasks.append(DatabricksSubmitRunOperator(
            task_id=('match_new_operators_with_integrated_operators_{}'.format(code)),
            existing_cluster_id=cluster_id,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'egg': egg}
            ],
            spark_python_task={
                'python_file': 'dbfs:/libraries/name_matching/join_new_operators_with_persistent_uuid.py',
                'parameters': [
                    '--integrated_operators_input_path', integrated_bucket.format(date=two_day_ago, fn='operators'),
                    '--ingested_daily_operators_input_path', ingested_bucket.format(date=one_day_ago, fn='operators'),
                    '--updated_integrated_output_path',
                    intermediate_bucket.format(date=one_day_ago, fn='updated_operators_integrated'),
                    '--unmatched_output_path', intermediate_bucket.format(date=one_day_ago, fn='operators_unmatched'),
                    '--country_code', code]
            }
        ))

    match_unmatched_operators = DatabricksSubmitRunOperator(
        task_id='match_unmatched_operators',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'egg': egg}
        ],
        spark_python_task={
            'python_file': 'dbfs:/libraries/name_matching/match_operators.py',
            'parameters': ['--input_file', intermediate_bucket.format(date=one_day_ago, fn='operators_unmatched'),
                           '--output_path', intermediate_bucket.format(date=one_day_ago, fn='operators_matched')]
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
            'parameters': [intermediate_bucket.format(date=one_day_ago, fn='operators_matched'),
                           ingested_bucket.format(date=one_day_ago, fn='operators_unmatched'),
                           intermediate_bucket.format(date=one_day_ago, fn='golden_records_new')]
        })

    update_golden_records = DatabricksSubmitRunOperator(
        task_id='update_golden_records',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorUpdateGoldenRecord",
            'parameters': [intermediate_bucket.format(date=one_day_ago, fn='updated_operators_integrated'),
                           intermediate_bucket.format(date=one_day_ago, fn='golden_records_updated')]
        }
    )

    combine_to_create_integrated = DatabricksSubmitRunOperator(
        task_id='combine_to_create_integrated',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.OperatorCombining",
            'parameters': [intermediate_bucket.format(date=one_day_ago, fn='golden_records_updated'),
                           intermediate_bucket.format(date=one_day_ago, fn='golden_records_new'),
                           integrated_bucket.format(date=one_day_ago, fn='operators')]
        }
    )

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
            'parameters': [integrated_bucket.format(date=one_day_ago, fn='operators'),
                           export_bucket.format(date=one_day_ago, fn='acm/operators.csv'),
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

    start_cluster >> uninstall_old_libraries >> operators_to_parquet
    for t in tasks:
        t.set_upstream([operators_to_parquet])
        t.set_downstream([update_golden_records, match_unmatched_operators])
    update_golden_records >> combine_to_create_integrated
    match_unmatched_operators >> merge_operators >> combine_to_create_integrated
    combine_to_create_integrated >> update_operators_table >> terminate_cluster
    combine_to_create_integrated >> operators_to_acm >> terminate_cluster
