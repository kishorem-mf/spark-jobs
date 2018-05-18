from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.hooks.base_hook import BaseHook
from airflow.operators.subdag_operator import SubDagOperator

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
    wasb_export_container, operator_country_codes

default_args.update(
    {'start_date': datetime(2017, 7, 12)}
)
interval = '@once'

with DAG('ohub_contact_person_first_ingest', default_args=default_args,
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

    contact_persons_file_interface_to_parquet = DatabricksSubmitRunOperator(
        task_id="contact_persons_file_interface_to_parquet",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.tsv2parquet.file_interface.ContactPersonConverter",
            'parameters': ['--inputFile', raw_bucket.format(date='{{ds}}',
                                                            schema='contactpersons',
                                                            channel='file_interface'),
                           '--outputFile', ingested_bucket.format(date='{{ds}}',
                                                                  fn='contactpersons',
                                                                  channel='file_interface'),
                           '--fieldSeparator', u"\u2030",
                           '--strictIngestion', "false",
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    contact_persons_exact_match = DatabricksSubmitRunOperator(
        task_id="contact_persons_exact_match",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonExactMatcher",
            'parameters': ['--inputFile', ingested_bucket.format(date='{{ds}}',
                                                                 fn='contactpersons',
                                                                 channel='file_interface'),
                           '--outputFile', intermediate_bucket.format(date='{{ds}}',
                                                                      fn='contactpersons_exact_match',
                                                                      channel='file_interface'),
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
            DatabricksSubmitRunOperator(
                dag=sub_dag,
                task_id='match_contacts_{}'.format(code),
                existing_cluster_id=cluster_id,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': 'dbfs:/libraries/name_matching/match_contacts.py',
                    'parameters': ['--input_file', ingested_bucket.format(date='{{ds}}',
                                                                          fn='contactpersons',
                                                                          channel='*'),
                                   '--output_path', intermediate_bucket.format(date='{{ds}}', fn='contacts_matched'),
                                   '--country_code', code]
                }
            )

        return sub_dag

    match_per_country = SubDagOperator(
        subdag=matching_sub_dag('ohub_contact_person_first_ingest', 'match_per_country', default_args['start_date'],
                                interval),
        task_id='match_per_country',
    )

    merge_contact_persons = DatabricksSubmitRunOperator(
        task_id='merge_contact_persons',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile', intermediate_bucket.format(date='{{ds}}', fn='contacts_matched'),
                           '--entityInputFile', ingested_bucket.format(date='{{ds}}',
                                                                       fn='contactpersons',
                                                                       channel='*'),
                           '--outputFile', intermediate_bucket.format(date='{{ds}}', fn='contactpersons'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    contact_person_combining = DatabricksSubmitRunOperator(
        task_id='contact_person_combining',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],

        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombining",
            'parameters': ['--integratedUpdated', intermediate_bucket.format(date='{{ds}}',
                                                                             fn='contactpersons_exact_match'),
                           '--newGolden', intermediate_bucket.format(date='{{ds}}',
                                                                     fn='contacts_matched',
                                                                     channel='*'),
                           '--combinedEntities', intermediate_bucket.format(date='{{ds}}',
                                                                            fn='contactpersons_combined'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    contact_person_referencing = DatabricksSubmitRunOperator(
        task_id='contact_person_referencing',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],

        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonReferencing",
            'parameters': ['--combinedInputFile', intermediate_bucket.format(date='{{ds}}',
                                                                             fn='contactpersons_combined'),
                           '--operatorInputFile', integrated_bucket.format(date='{{ds}}',
                                                                           fn='operators',
                                                                           channel='*'),
                           '--outputFile', integrated_bucket.format(date='{{ds}}', fn='contactpersons'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    op_file = 'acm/UFS_RECIPIENTS_{{ds_nodash}}000000.csv'

    contact_persons_to_acm = DatabricksSubmitRunOperator(
        task_id="contact_persons_to_acm",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.ContactPersonAcmConverter",
            'parameters': ['--inputFile', integrated_bucket.format(date='{{ds}}', fn='contactpersons'),
                           '--outputFile', export_bucket.format(date='{{ds}}', fn=op_file),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    contact_person_ftp_to_acm = SFTPOperator(
        task_id='contact_person_ftp_to_acm',
        local_filepath=wasb_export_container.format(date='{{ds}}', fn=op_file),
        remote_filepath='/incoming/UFS_upload_folder/',
        ssh_conn_id='acm_sftp_ssh',
        operation=SFTPOperation.PUT)

    start_cluster >> uninstall_old_libraries >> contact_persons_file_interface_to_parquet >> contact_persons_exact_match
    contact_persons_exact_match >> match_per_country >> merge_contact_persons >> contact_person_combining
    contact_person_combining >> contact_person_referencing >> contact_persons_to_acm
    contact_persons_to_acm >> terminate_cluster
    contact_persons_to_acm >> contact_person_ftp_to_acm
