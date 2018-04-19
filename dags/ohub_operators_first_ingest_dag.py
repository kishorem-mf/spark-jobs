from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook

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
    {'start_date': datetime(2017, 7, 12)}
)

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
            'parameters': [raw_bucket.format(date='{{ds}}', schema='operators'),
                           ingested_bucket.format(date='{{ds}}', fn='operators'),
                           "false"]
        }
    )

    tasks = []
    for code in operator_country_codes:
        tasks.append(DatabricksSubmitRunOperator(
            task_id='match_operators_{}'.format(code),
            existing_cluster_id=cluster_id,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'egg': egg}
            ],
            spark_python_task={
                'python_file': 'dbfs:/libraries/name_matching/match_operators.py',
                'parameters': ['--input_file', ingested_bucket.format(date='{{ds}}', fn='operators'),
                               '--output_path', intermediate_bucket.format(date='{{ds}}', fn='operators_matched'),
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

    start_cluster >> uninstall_old_libraries >> operators_to_parquet
    for t in tasks:
        t.set_upstream([operators_to_parquet])
        t.set_downstream([merge_operators])
    merge_operators >> operators_to_acm >> terminate_cluster
