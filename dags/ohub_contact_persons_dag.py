from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.hooks.base_hook import BaseHook
from airflow.operators.subdag_operator import SubDagOperator

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from custom_operators.file_from_wasb import FileFromWasbOperator
from ohub_dag_config import \
    default_args, \
    databricks_conn_id, \
    jar, egg, \
    raw_bucket, ingested_bucket, intermediate_bucket, integrated_bucket, export_bucket, \
    wasb_export_container, operator_country_codes, interval, one_day_ago, two_day_ago, create_cluster, \
    terminate_cluster, default_cluster_config

default_args.update(
    {'start_date': datetime(2018, 5, 30)}
)

cluster_name = "ohub_contact_persons_{{ds}}"

with DAG('ohub_contact_persons', default_args=default_args,
         schedule_interval=interval) as dag:
    create_cluster_contact_persons = create_cluster('create_clusters_contact_persons',
                                                    default_cluster_config(cluster_name))
    terminate_cluster_contact_persons = terminate_cluster('terminate_cluster_contact_persons',
                                                          cluster_name)

    postgres_connection = BaseHook.get_connection('postgres_channels')
    postgres_config = [
        '--postgressUrl', postgres_connection.host,
        '--postgressUsername', postgres_connection.login,
        '--postgressPassword', postgres_connection.password,
        '--postgressDB', postgres_connection.schema
    ]

    contact_persons_file_interface_to_parquet = DatabricksSubmitRunOperator(
        task_id="contact_persons_file_interface_to_parquet",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.tsv2parquet.file_interface.ContactPersonConverter",
            'parameters': ['--inputFile',
                           raw_bucket.format(date=one_day_ago, schema='contactpersons', channel='file_interface'),
                           '--outputFile',
                           ingested_bucket.format(date=one_day_ago, fn='contactpersons', channel='file_interface'),
                           '--strictIngestion', "false"] + postgres_config
        }
    )

    contact_persons_pre_processed = DatabricksSubmitRunOperator(
        task_id="contact_persons_pre_processed",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonPreProcess",
            'parameters': ['--integratedInputFile',
                           integrated_bucket.format(date=one_day_ago, schema='contact_persons'),
                           '--deltaInputFile',
                           ingested_bucket.format(date=one_day_ago, fn='contactpersons', channel='*'),
                           '--deltaPreProcessedOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_pre_processed'),
                           "false"]
        }
    )

    exact_match_integrated_ingested = DatabricksSubmitRunOperator(
        task_id="exact_match_integrated_ingested",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonIntegratedExactMatch",
            'parameters': ['--integratedInputFile',
                           integrated_bucket.format(date=two_day_ago, fn='contact_persons'),
                           '--deltaInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_pre_processed'),
                           '--matchedExactOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_person_exact_matches'),
                           '--unmatchedIntegratedOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_person_unmatched_integrated'),
                           '--unmatchedDeltaOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_person_unmatched_delta')
                           ] + postgres_config
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
                task_id=('match_new_contact_persons_with_integrated_operators_{}'.format(code)),
                cluster_name=cluster_name,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': 'dbfs:/libraries/name_matching/delta_contacts.py',
                    'parameters': [
                        '--integrated_input_path',
                        intermediate_bucket.format(date=one_day_ago, fn='contact_person_unmatched_integrated'),
                        '--ingested_daily_input_path',
                        intermediate_bucket.format(date=one_day_ago, fn='contact_person_unmatched_delta'),
                        '--updated_integrated_output_path',
                        intermediate_bucket.format(date=one_day_ago,
                                                   fn='contact_persons_fuzzy_matched_delta_integrated'),
                        '--unmatched_output_path',
                        intermediate_bucket.format(date=one_day_ago, fn='contact_persons_delta_left_overs'),
                        '--country_code', code]
                }
            )

            match_unmatched = DatabricksSubmitRunOperator(
                dag=sub_dag,
                task_id='match_unmatched_contact_persons_{}'.format(code),
                cluster_name=cluster_name,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': 'dbfs:/libraries/name_matching/match_contacts.py',
                    'parameters': ['--input_file',
                                   intermediate_bucket.format(date=one_day_ago, fn='contact_persons_delta_left_overs'),
                                   '--output_path',
                                   intermediate_bucket.format(date=one_day_ago,
                                                              fn='contacts_persons_fuzzy_matched_delta'),
                                   '--country_code', code]
                }
            )
            match_new >> match_unmatched

        return sub_dag

    match_per_country = SubDagOperator(
        subdag=matching_sub_dag('ohub_contact_persons', 'match_per_country', default_args['start_date'], interval),
        task_id='match_per_country',
    )

    join_matched_contact_persons = DatabricksSubmitRunOperator(
        task_id='join_matched_contact_persons',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contacts_persons_fuzzy_matched_delta'),
                           '--entityInputFile', intermediate_bucket.format(date=one_day_ago,
                                                                           fn='contact_persons_delta_left_overs'),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='contact_persons_delta_golden_records')] + postgres_config
        }
    )

    join_fuzzy_and_exact_matched = DatabricksSubmitRunOperator(
        task_id='join_fuzzy_and_exact_matched_contact_persons',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombineExactAndFuzzyMatches",
            'parameters': ['--contactPersonExactMatchedInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='contact_person_exact_matches'),
                           '--contactPersonFuzzyMatchedDeltaIntegratedInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='contact_persons_fuzzy_matched_delta_integrated'),
                           '--contactPersonsDeltaGoldenRecordsInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='contact_persons_delta_golden_records'),
                           '--contactPersonsCombinedOutputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='contact_persons_combined')]
        }
    )

    update_golden_records = DatabricksSubmitRunOperator(
        task_id='update_golden_records',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_combined'),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_updated_golden_records')
                           ] + postgres_config
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
            'parameters': ['--combinedInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_updated_golden_records'),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago,
                                                                           fn='operators',
                                                                           channel='*'),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn='contact_persons'),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn='contact_persons')] + postgres_config
        }
    )

    op_file = 'acm/UFS_RECIPIENTS_{{ds_nodash}}000000.csv'

    contact_persons_to_acm = DatabricksSubmitRunOperator(
        task_id="contact_persons_to_acm",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.ContactPersonAcmConverter",
            'parameters': ['--inputFile', integrated_bucket.format(date=one_day_ago, fn='contact_persons'),
                           '--outputFile', export_bucket.format(date=one_day_ago, fn=op_file)
                           ] + postgres_config
        }
    )

    tmp_file = '/tmp/' + op_file

    contact_persons_acm_from_wasb = FileFromWasbOperator(
        task_id='contact_persons_acm_from_wasb',
        file_path=tmp_file,
        container_name='prod',
        wasb_conn_id='azure_blob',
        blob_name=wasb_export_container.format(date='{{ds}}', fn=op_file)
    )

    contact_persons_ftp_to_acm = SFTPOperator(
        task_id='contact_persons_ftp_to_acm',
        local_filepath=tmp_file,
        remote_filepath='/incoming/temp/ohub_2_test/{}'.format(tmp_file.split('/')[-1]),
        ssh_conn_id='acm_sftp_ssh',
        operation=SFTPOperation.PUT
    )

    create_cluster_contact_persons >> contact_persons_file_interface_to_parquet >> contact_persons_pre_processed
    contact_persons_pre_processed >> exact_match_integrated_ingested

    exact_match_integrated_ingested >> match_per_country
    exact_match_integrated_ingested >> join_fuzzy_and_exact_matched

    match_per_country >> join_matched_contact_persons >> join_fuzzy_and_exact_matched

    join_fuzzy_and_exact_matched >> update_golden_records >> contact_person_referencing >> contact_persons_to_acm
    contact_persons_to_acm >> terminate_cluster_contact_persons
    contact_persons_to_acm >> contact_persons_acm_from_wasb >> contact_persons_ftp_to_acm
