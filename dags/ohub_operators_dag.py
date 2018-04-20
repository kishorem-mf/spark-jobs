from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from datetime import datetime

from custom_operators.databricks_functions import \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator, \
    DatabricksStartClusterOperator, \
    DatabricksUninstallLibrariesOperator
from operators_config import \
    default_args, \
    cluster_id, databricks_conn_id, \
    jar, egg, \
    raw_bucket, ingested_bucket, intermediate_bucket, integrated_bucket, export_bucket, \
    operator_country_codes

default_args.update(
    {'start_date': datetime(2018, 4, 6)}
)

one_day_ago = '2018-04-06'  # should become {{ ds }}
two_day_ago = '2017-07-12'  # should become {{ macros.ds_add(ds, -1) }}

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
            'parameters': [raw_bucket.format(date=one_day_ago, schema='operators'),
                           ingested_bucket.format(date=one_day_ago, fn='operators'),
                           "false"]
        }
    )

    match_new_tasks = []
    for code in operator_country_codes:
        match_new_tasks.append(DatabricksSubmitRunOperator(
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

    match_unmatched_tasks = []
    for code in operator_country_codes:
        match_unmatched_tasks.append(DatabricksSubmitRunOperator(
            task_id='match_unmatched_operators_{}'.format(code),
            existing_cluster_id=cluster_id,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'egg': egg}
            ],
            spark_python_task={
                'python_file': 'dbfs:/libraries/name_matching/match_operators.py',
                'parameters': ['--input_file', intermediate_bucket.format(date=one_day_ago, fn='operators_unmatched'),
                               '--output_path', intermediate_bucket.format(date=one_day_ago, fn='operators_matched'),
                               '--country_code', code]
            }
        ))

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
                           intermediate_bucket.format(date=one_day_ago, fn='operators_unmatched'),
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
    for i, _ in enumerate(operator_country_codes):
        match_new_tasks[i].set_upstream([operators_to_parquet])
        match_new_tasks[i].set_downstream([update_golden_records, match_unmatched_tasks[i]])
        match_unmatched_tasks[i].set_downstream([merge_operators])
    update_golden_records >> combine_to_create_integrated
    merge_operators >> combine_to_create_integrated
    combine_to_create_integrated >> update_operators_table >> terminate_cluster
    combine_to_create_integrated >> operators_to_acm >> terminate_cluster
