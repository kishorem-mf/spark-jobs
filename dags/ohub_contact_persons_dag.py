from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.operators.bash_operator import BashOperator

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from custom_operators.empty_fallback import EmptyFallbackOperator
from custom_operators.file_from_wasb import FileFromWasbOperator
from ohub_dag_config import \
    default_args, databricks_conn_id, jar, container_name, \
    ingested_bucket, intermediate_bucket, integrated_bucket, export_bucket, \
    wasb_raw_container, wasb_export_container, interval, one_day_ago, two_day_ago, wasb_conn_id, \
    create_cluster, \
    terminate_cluster, default_cluster_config, ingest_task, postgres_config, delta_fuzzy_matching_tasks

default_args.update(
    {
        'start_date': datetime(2018, 6, 4),
        'pool': 'ohub_contactpersons_pool'
    }
)

schema = 'contactpersons'
cluster_name = "ohub_contactpersons_{{ds}}"

with DAG('ohub_{}'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    contactpersons_create_cluster = create_cluster('{}_create_clusters'.format(schema),
                                                   default_cluster_config(cluster_name))
    contactpersons_terminate_cluster = terminate_cluster('{}_terminate_cluster'.format(schema), cluster_name)

    empty_fallback = EmptyFallbackOperator(
        task_id='{}_empty_fallback'.format(schema),
        container_name=container_name,
        file_path=wasb_raw_container.format(date=one_day_ago, schema=schema, channel='file_interface'),
        wasb_conn_id=wasb_conn_id)

    contact_persons_file_interface_to_parquet = ingest_task(
        schema=schema,
        channel='file_interface',
        clazz="com.unilever.ohub.spark.tsv2parquet.file_interface.ContactPersonConverter",
        cluster_name=cluster_name
    )

    contact_persons_pre_processed = DatabricksSubmitRunOperator(
        task_id="{}_pre_processed".format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonPreProcess",
            'parameters': ['--integratedInputFile',
                           integrated_bucket.format(date=two_day_ago, fn=schema),
                           '--deltaInputFile',
                           ingested_bucket.format(date=one_day_ago, fn=schema, channel='*'),
                           '--deltaPreProcessedOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_pre_processed'.format(schema))]
        }
    )

    exact_match_integrated_ingested = DatabricksSubmitRunOperator(
        task_id="{}_exact_match_integrated_ingested".format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonIntegratedExactMatch",
            'parameters': ['--integratedInputFile',
                           integrated_bucket.format(date=two_day_ago, fn=schema),
                           '--deltaInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_pre_processed'.format(schema)),
                           '--matchedExactOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_exact_matches'.format(schema)),
                           '--unmatchedIntegratedOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_integrated'.format(schema)),
                           '--unmatchedDeltaOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_delta'.format(schema))
                           ] + postgres_config
        }
    )

    begin_fuzzy_matching = BashOperator(
        task_id='{}_start_fuzzy_matching'.format(schema),
        bash_command='echo "start fuzzy matching"',
    )

    matching_tasks = delta_fuzzy_matching_tasks(
        schema=schema,
        cluster_name=cluster_name,
        delta_match_py='dbfs:/libraries/name_matching/delta_contacts.py',
        match_py='dbfs:/libraries/name_matching/match_contacts.py',
        integrated_input=intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_integrated'.format(schema)),
        ingested_input=intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_delta'.format(schema))
    )

    end_fuzzy_matching = BashOperator(
        task_id='{}_end_fuzzy_matching'.format(schema),
        bash_command='echo "end fuzzy matching"',
    )

    join_matched_contact_persons = DatabricksSubmitRunOperator(
        task_id='{}_join_matched'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_fuzzy_matched_delta'.format(schema)),
                           '--entityInputFile', intermediate_bucket.format(date=one_day_ago,
                                                                           fn='{}_delta_left_overs'.format(schema)),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_delta_golden_records'.format(schema))] + postgres_config
        }
    )

    join_fuzzy_and_exact_matched = DatabricksSubmitRunOperator(
        task_id='{}_join_fuzzy_and_exact_matched'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombineExactAndFuzzyMatches",
            'parameters': ['--contactPersonExactMatchedInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_exact_matches'.format(schema)),
                           '--contactPersonFuzzyMatchedDeltaIntegratedInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_fuzzy_matched_delta_integrated'.format(schema)),
                           '--contactPersonsDeltaGoldenRecordsInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_delta_golden_records'.format(schema)),
                           '--contactPersonsCombinedOutputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_combined'.format(schema))]
        }
    )

    update_golden_records = DatabricksSubmitRunOperator(
        task_id='{}_update_golden_records'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(schema)),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_updated_golden_records'.format(schema))
                           ] + postgres_config
        }
    )

    contact_person_referencing = DatabricksSubmitRunOperator(
        task_id='{}_referencing'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonReferencing",
            'parameters': ['--combinedInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_updated_golden_records'.format(schema)),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=schema)]
        }
    )

    op_file = 'acm/UFS_RECIPIENTS_{{ds_nodash}}000000.csv'

    contact_persons_to_acm = DatabricksSubmitRunOperator(
        task_id='{}_to_acm'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.ContactPersonAcmConverter",
            'parameters': ['--inputFile', integrated_bucket.format(date=one_day_ago, fn=schema),
                           '--outputFile', export_bucket.format(date=one_day_ago, fn=op_file)
                           ] + postgres_config
        }
    )

    tmp_file = '/tmp/' + op_file

    contact_persons_acm_from_wasb = FileFromWasbOperator(
        task_id='{}_acm_from_wasb'.format(schema),
        file_path=tmp_file,
        container_name=container_name,
        wasb_conn_id=wasb_conn_id,
        blob_name=wasb_export_container.format(date='{{ds}}', fn=op_file)
    )

    contact_persons_ftp_to_acm = SFTPOperator(
        task_id='{}_ftp_to_acm'.format(schema),
        local_filepath=tmp_file,
        remote_filepath='/incoming/temp/ohub_2_test/{}'.format(tmp_file.split('/')[-1]),
        ssh_conn_id='acm_sftp_ssh',
        operation=SFTPOperation.PUT
    )

    empty_fallback >> contact_persons_file_interface_to_parquet
    contactpersons_create_cluster >> contact_persons_file_interface_to_parquet
    contact_persons_file_interface_to_parquet >> contact_persons_pre_processed >> exact_match_integrated_ingested

    exact_match_integrated_ingested >> begin_fuzzy_matching
    exact_match_integrated_ingested >> join_fuzzy_and_exact_matched
    for t in matching_tasks['in']:
        begin_fuzzy_matching >> t
    for t in matching_tasks['out']:
        t >> end_fuzzy_matching

    end_fuzzy_matching >> join_matched_contact_persons >> join_fuzzy_and_exact_matched

    join_fuzzy_and_exact_matched >> update_golden_records >> contact_person_referencing >> contact_persons_to_acm
    contact_persons_to_acm >> contactpersons_terminate_cluster
    contact_persons_to_acm >> contact_persons_acm_from_wasb >> contact_persons_ftp_to_acm
