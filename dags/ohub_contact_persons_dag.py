from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub_dag_config import \
    default_args, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, interval, \
    one_day_ago, two_day_ago, \
    postgres_config, GenericPipeline, SubPipeline

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
    generic = (
        GenericPipeline(schema=schema, cluster_name=cluster_name, clazz='ContactPerson')
            .is_delta()
            .has_export_to_acm(acm_schema_name='UFS_RECIPIENTS')
            .has_ingest_from_file_interface()
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        delta_match_py='dbfs:/libraries/name_matching/delta_contacts.py',
        match_py='dbfs:/libraries/name_matching/match_contacts.py',
        integrated_input=intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_integrated'.format(schema)),
        ingest_input=intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_delta'.format(schema))
    )

    pre_processing = DatabricksSubmitRunOperator(
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

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id='operators_integrated_sensor',
        external_dag_id='ohub_operators',
        external_task_id='operators_update_golden_records'
    )

    referencing = DatabricksSubmitRunOperator(
        task_id='{}_referencing'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonReferencing",
            'parameters': ['--combinedInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(schema)),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_updated_references'.format(schema))]
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
                           intermediate_bucket.format(date=one_day_ago, fn='{}_updated_references'.format(schema)),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn=schema)] + postgres_config
        }
    )

    ingest.last_task >> pre_processing >> exact_match_integrated_ingested
    exact_match_integrated_ingested >> fuzzy_matching.first_task >> join_fuzzy_and_exact_matched
    join_fuzzy_and_exact_matched >> operators_integrated_sensor >> referencing >> update_golden_records >> export.first_task
