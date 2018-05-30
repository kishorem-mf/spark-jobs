from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.hooks.base_hook import BaseHook
from airflow.operators.subdag_operator import SubDagOperator

from custom_operators.databricks_functions import \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator, \
    DatabricksCreateClusterOperator
from custom_operators.file_from_wasb import FileFromWasbOperator
from ohub_dag_config import \
    default_args, \
    databricks_conn_id, \
    jar, egg, \
    raw_bucket, ingested_bucket, intermediate_bucket, integrated_bucket, export_bucket, \
    wasb_export_container, operator_country_codes

default_args.update(
    {'start_date': datetime(2017, 7, 12)}
)
interval = '@once'

utc_now = datetime.utcnow().strftime('%Y%m%d')
cluster_name = "ohub_contact_persons_first_{}".format(utc_now)

with DAG('ohub_contact_person_first_ingest', default_args=default_args,
         schedule_interval=interval) as dag:

    create_cluster = DatabricksCreateClusterOperator(
        task_id='create_cluster',
        databricks_conn_id=databricks_conn_id,
        cluster_config={
            "cluster_name": cluster_name,
            "spark_version": "4.0.x-scala2.11",
            "node_type_id": "Standard_DS5_v2",
            "autoscale": {
                "min_workers": 4,
                "max_workers": 12
            },
            "autotermination_minutes": 30,
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
            },
        }
    )

    terminate_cluster = DatabricksTerminateClusterOperator(
        task_id='terminate_cluster',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id
    )

    postgres_connection = BaseHook.get_connection('postgres_channels')

    contact_persons_file_interface_to_parquet = DatabricksSubmitRunOperator(
        task_id="contact_persons_file_interface_to_parquet",
        cluster_name=cluster_name,
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
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonExactMatcher",
            'parameters': ['--inputFile', ingested_bucket.format(date='{{ds}}',
                                                                 fn='contactpersons',
                                                                 channel='file_interface'),
                           '--exactMatchOutputFile', intermediate_bucket.format(date='{{ds}}',
                                                                                fn='contactpersons_exact_match'),
                           '--leftOversOutputFile', intermediate_bucket.format(date='{{ds}}',
                                                                               fn='contactpersons_left_overs'),
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
                cluster_name=cluster_name,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': 'dbfs:/libraries/name_matching/match_contacts.py',
                    'parameters': ['--input_file', intermediate_bucket.format(date='{{ds}}',
                                                                              fn='contactpersons_left_overs'),
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
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile', intermediate_bucket.format(date='{{ds}}', fn='contacts_matched'),
                           '--entityInputFile', intermediate_bucket.format(date='{{ds}}',
                                                                           fn='contactpersons_left_overs'),
                           '--outputFile', intermediate_bucket.format(date='{{ds}}', fn='contactpersons'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    contact_person_combining = DatabricksSubmitRunOperator(
        task_id='contact_person_combining',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],

        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombining",
            'parameters': ['--integratedUpdated', intermediate_bucket.format(date='{{ds}}',
                                                                             fn='contactpersons_exact_match'),
                           '--newGolden', intermediate_bucket.format(date='{{ds}}',
                                                                     fn='contactpersons',
                                                                     channel='*'),
                           '--combinedEntities', intermediate_bucket.format(date='{{ds}}',
                                                                            fn='contactpersons_combined')]
        }
    )

    contact_person_referencing = DatabricksSubmitRunOperator(
        task_id='contact_person_referencing',
        cluster_name=cluster_name,
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
                           '--outputFile', integrated_bucket.format(date='{{ds}}', fn='contactpersons')]
        }
    )

    cp_file = 'acm/UFS_RECIPIENTS_{{ds_nodash}}000000.csv'

    contact_persons_to_acm = DatabricksSubmitRunOperator(
        task_id="contact_persons_to_acm",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.ContactPersonAcmConverter",
            'parameters': ['--inputFile', integrated_bucket.format(date='{{ds}}', fn='contactpersons'),
                           '--outputFile', export_bucket.format(date='{{ds}}', fn=cp_file)]
        }
    )

    tmp_file = '/tmp/' + cp_file

    contact_persons_acm_from_wasb = FileFromWasbOperator(
        task_id='contact_persons_acm_from_wasb',
        file_path=tmp_file,
        container_name='prod',
        wasb_conn_id='azure_blob',
        blob_name=wasb_export_container.format(date='{{ds}}', fn=cp_file)
    )

    contact_person_ftp_to_acm = SFTPOperator(
        task_id='contact_person_ftp_to_acm',
        local_filepath=tmp_file,
        remote_filepath='/incoming/temp/ohub_2_test/{}'.format(tmp_file.split('/')[-1]),
        ssh_conn_id='acm_sftp_ssh',
        operation=SFTPOperation.PUT
    )

    create_cluster >> contact_persons_file_interface_to_parquet >> contact_persons_exact_match >> match_per_country
    match_per_country >> merge_contact_persons >> contact_person_combining >> contact_person_referencing
    contact_person_referencing >> contact_persons_to_acm >> contact_persons_acm_from_wasb >> contact_person_ftp_to_acm
    contact_person_ftp_to_acm >> terminate_cluster
