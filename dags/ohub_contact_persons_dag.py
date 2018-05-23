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
from operators_config import \
    default_args, \
    cluster_id, databricks_conn_id, \
    jar, egg, \
    raw_bucket, ingested_bucket, intermediate_bucket, integrated_bucket, export_bucket, \
    wasb_export_container, operator_country_codes

default_args.update(
    {'start_date': datetime(2018, 5, 17)}
)

interval = '@once'
one_day_ago = '{{ds}}'
two_day_ago = '{{yesterday_ds}}'

utc_now = datetime.utcnow().strftime('%Y%m%d_%H%m%S')
cluster_name = "ohub_contact_persons_{}".format(utc_now)

with DAG('ohub_contact_persons', default_args=default_args,
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
    postgres_config = [
        '--postgressUrl', postgres_connection.host,
        '--postgressUsername', postgres_connection.login,
        '--postgressPassword', postgres_connection.password,
        '--postgressDB', postgres_connection.schema
    ]

    contact_persons_file_interface_to_parquet = DatabricksSubmitRunOperator(
        task_id="contact_persons_file_interface_to_parquet",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.tsv2parquet.file_interface.ContactPersonConverter",
            'parameters': ['--inputFile', raw_bucket.format(date=one_day_ago,
                                                            schema='contactpersons',
                                                            channel='file_interface'),
                           '--outputFile', ingested_bucket.format(date=one_day_ago,
                                                                  fn='contactpersons',
                                                                  channel='file_interface'),
                           '--fieldSeparator', u"\u2030",
                           '--strictIngestion', "false"] + postgres_config
        }
    )

    exact_match_integrated_ingested = DatabricksSubmitRunOperator(
        task_id="exact_match_integrated_ingested",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.??",
            'parameters': ['--ingested', ingested_bucket.format(date=one_day_ago,
                                                                fn='contact_persons',
                                                                channel='file_interface'),
                           '--integrated', integrated_bucket.format(date=two_day_ago,
                                                                    fn='contactpersons'),
                           '--matchedOutputFile', intermediate_bucket.format(date=one_day_ago,
                                                                             fn='contact_person_updated_exact_matches'),
                           '--unmatchedOutputFile', intermediate_bucket.format(date=one_day_ago,
                                                                               fn='contact_person_no_exact_match')
                           ]
        }
    )

    exact_match_ingested = DatabricksSubmitRunOperator(
        task_id="exact_match_ingested",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonExactMatcher",
            'parameters': ['--inputFile', intermediate_bucket.format(date=one_day_ago,
                                                                     fn='contact_person_no_exact_match'),
                           '--matchedOutputFile', intermediate_bucket.format(date=one_day_ago,
                                                                             fn='contact_person_ingested_exact_match'),
                           '--unmatchedOutputFile', intermediate_bucket.format(date=one_day_ago,
                                                                               fn='contact_person_left_overs')
                           ]
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
                existing_cluster_id=cluster_id,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': 'dbfs:/libraries/name_matching/delta_contacts.py',
                    'parameters': [
                        '--integrated_contact_persons_input_path',
                        integrated_bucket.format(date=two_day_ago, fn='contact_persons'),
                        '--ingested_daily_contact_persons_input_path',
                        intermediate_bucket.format(date=one_day_ago,
                                                   fn='contact_persons_left_overs'),
                        '--updated_integrated_output_path',
                        intermediate_bucket.format(date=one_day_ago, fn='contact_persons_updated_integrated'),
                        '--unmatched_output_path',
                        intermediate_bucket.format(date=one_day_ago, fn='contact_persons_unmatched'),
                        '--country_code', code]
                }
            )
            match_unmatched = DatabricksSubmitRunOperator(
                dag=sub_dag,
                task_id='match_unmatched_contact_persons_{}'.format(code),
                existing_cluster_id=cluster_id,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': 'dbfs:/libraries/name_matching/match_contacts.py',
                    'parameters': ['--input_file',
                                   intermediate_bucket.format(date=one_day_ago, fn='contact_persons_unmatched'),
                                   '--output_path',
                                   intermediate_bucket.format(date=one_day_ago, fn='contact_persons_matched'),
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
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contacts_persons_matched'),
                           '--entityInputFile', ingested_bucket.format(date=one_day_ago,
                                                                       fn='contact_persons',
                                                                       channel='*'),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='contact_persons_golden_records_new')] + postgres_config
        }
    )

    combine_name_matching_results = DatabricksSubmitRunOperator(
        task_id='combine_name_matching_results',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombining",
            'parameters': ['--integratedUpdated',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_updated_integrated'),
                           '--newGolden',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_golden_records_new'),
                           '--combinedOperators',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_combined')]
        }
    )

    join_fuzzy_and_exact_matched = DatabricksSubmitRunOperator(
        task_id='join_fuzzy_and_exact_matched_contact_persons',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.??",
            'parameters': ['--nameMatched', intermediate_bucket.format(date=one_day_ago, fn='contacts_persons_matched'),
                           '--ingestedExactMatched', intermediate_bucket.format(date=one_day_ago,
                                                                                fn='contact_persons'),
                           '--integratedExactMatched', intermediate_bucket.format(date=one_day_ago,
                                                                                  fn='contact_persons'),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='contact_persons_combined')] + postgres_config
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
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_combined'),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn='operators')] + postgres_config
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
            'parameters': ['--combinedInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='contact_persons_combined'),
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
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.ContactPersonAcmConverter",
            'parameters': ['--inputFile', integrated_bucket.format(date=one_day_ago, fn='contact_persons'),
                           '--outputFile', export_bucket.format(date=one_day_ago, fn=op_file),

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

    create_cluster >> contact_persons_file_interface_to_parquet >> exact_match_integrated_ingested
    exact_match_integrated_ingested >> exact_match_ingested
    exact_match_integrated_ingested >> join_fuzzy_and_exact_matched

    exact_match_ingested >> join_fuzzy_and_exact_matched
    exact_match_ingested >> match_per_country >> join_matched_contact_persons >> combine_name_matching_results
    combine_name_matching_results >> join_fuzzy_and_exact_matched

    join_fuzzy_and_exact_matched >> update_golden_records >> contact_person_referencing >> contact_persons_to_acm
    contact_persons_to_acm >> terminate_cluster
    contact_persons_to_acm >> contact_persons_acm_from_wasb >> contact_persons_ftp_to_acm
