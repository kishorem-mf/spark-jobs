from airflow import DAG
from datetime import datetime

from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation

from airflow.hooks.base_hook import BaseHook

from custom_operators.file_from_wasb import FileFromWasbOperator
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
    wasb_raw_container, wasb_ingested_container, \
    wasb_intermediate_container, wasb_integrated_container, \
    wasb_export_container, operator_country_codes

default_args.update(
    {'start_date': datetime(2018, 5, 17)}
)

one_day_ago = '{{ds}}'
two_day_ago = '{{yesterday_ds}}'
interval = '@once'
wasb_conn_id = 'azure_blob'

with DAG('ohub_operators', default_args=default_args,
         schedule_interval=interval) as dag:
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

    postgres_connection = BaseHook.get_connection('postgres_channels')

    operators_file_interface_to_parquet = DatabricksSubmitRunOperator(
        task_id="operators_file_interface_to_parquet",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.tsv2parquet.file_interface.OperatorConverter",
            'parameters': ['--inputFile', raw_bucket.format(date=one_day_ago,
                                                            schema='operators',
                                                            channel='file_interface'),
                           '--outputFile', ingested_bucket.format(date=one_day_ago,
                                                                  fn='operators',
                                                                  channel='file_interface'),
                           '--strictIngestion', "false",
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    def matching_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
        sub_dag = DAG(
            '%s.%s' % (parent_dag_name, child_dag_name),
            schedule_interval=schedule_interval,
            start_date=start_date,
            default_args=default_args
        )

        for code in operator_country_codes:
            match_new = DatabricksSubmitRunOperator(
                dag=sub_dag,
                task_id=('match_new_operators_with_integrated_operators_{}'.format(code)),
                existing_cluster_id=cluster_id,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': 'dbfs:/libraries/name_matching/delta_operators.py',
                    'parameters': [
                        '--integrated_operators_input_path', integrated_bucket.format(date=two_day_ago, fn='operators'),
                        '--ingested_daily_operators_input_path',
                        ingested_bucket.format(date=one_day_ago,
                                               fn='operators',
                                               channel='*'),
                        '--updated_integrated_output_path',
                        intermediate_bucket.format(date=one_day_ago, fn='updated_operators_integrated'),
                        '--unmatched_output_path',
                        intermediate_bucket.format(date=one_day_ago, fn='operators_unmatched'),
                        '--country_code', code]
                }
            )
            match_unmatched = DatabricksSubmitRunOperator(
                dag=sub_dag,
                task_id='match_unmatched_operators_{}'.format(code),
                existing_cluster_id=cluster_id,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': 'dbfs:/libraries/name_matching/match_operators.py',
                    'parameters': ['--input_file',
                                   intermediate_bucket.format(date=one_day_ago, fn='operators_unmatched'),
                                   '--output_path',
                                   intermediate_bucket.format(date=one_day_ago, fn='operators_matched'),
                                   '--country_code', code]
                }
            )
            match_new >> match_unmatched

        return sub_dag

    match_per_country = SubDagOperator(
        subdag=matching_sub_dag('ohub_operators', 'match_per_country', default_args['start_date'], interval),
        task_id='match_per_country',
    )

    merge_operators = DatabricksSubmitRunOperator(
        task_id='merge_operators',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='operators_matched'),
                           '--entityInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='operators_unmatched'),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='golden_records_new'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        })

    combine_to_create_integrated = DatabricksSubmitRunOperator(
        task_id='combine_to_create_integrated',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.OperatorCombining",
            'parameters': ['--integratedUpdated',
                           intermediate_bucket.format(date=one_day_ago, fn='updated_operators_integrated'),
                           '--newGolden',
                           intermediate_bucket.format(date=one_day_ago, fn='golden_records_new'),
                           '--combinedOperators',
                           intermediate_bucket.format(date=one_day_ago, fn='operators_combined')]
        }
    )

    update_golden_records = DatabricksSubmitRunOperator(
        task_id='update_golden_records',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='operators_combined'),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    op_file = 'acm/UFS_OPERATORS_{{ds_nodash}}000000.csv'

    operators_to_acm = DatabricksSubmitRunOperator(
        task_id="operators_to_acm",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.OperatorAcmConverter",
            'parameters': ['--inputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile', export_bucket.format(date=one_day_ago, fn=op_file),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn='operators'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    tmp_file = '/tmp/' + op_file

    operators_acm_from_wasb = FileFromWasbOperator(
        task_id='operators_acm_from_wasb',
        file_path=tmp_file,
        container_name=wasb_export_container.format(date=one_day_ago, fn=op_file),
        wasb_conn_id='azure_blob',
        blob_name='prod'
    )

    operators_ftp_to_acm = SFTPOperator(
        task_id='operators_ftp_to_acm',
        local_filepath=tmp_file,
        remote_filepath='/incoming/UFS_upload_folder/',
        ssh_conn_id='acm_sftp_ssh',
        operation=SFTPOperation.PUT)

    update_operators_table = DatabricksSubmitRunOperator(
        task_id='update_operators_table',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        notebook_task={'notebook_path': '/Users/tim.vancann@unilever.com/update_integrated_tables'}
    )

    start_cluster >> uninstall_old_libraries >> operators_file_interface_to_parquet >> match_per_country
    match_per_country >> combine_to_create_integrated
    match_per_country >> merge_operators >> combine_to_create_integrated
    combine_to_create_integrated >> update_golden_records >> update_operators_table >> terminate_cluster
    update_golden_records >> operators_to_acm >> terminate_cluster
    operators_to_acm >> operators_acm_from_wasb >> operators_ftp_to_acm
