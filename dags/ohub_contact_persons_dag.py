from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub_dag_config import \
    default_args, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, one_day_ago, \
    two_day_ago, postgres_config, GenericPipeline, SubPipeline, DagConfig, large_cluster_config

default_args.update(
    {
        'start_date': datetime(2018, 6, 4),
        'pool': 'ohub_contactpersons_pool'
    }
)

entity = 'contactpersons'
dag_config = DagConfig(entity, is_delta=True, cluster_config=large_cluster_config)

with DAG(dag_config.dag_id, default_args=default_args, schedule_interval=dag_config.schedule) as dag:
    generic = (
        GenericPipeline(dag_config, class_prefix='ContactPerson')
            .has_export_to_acm(acm_schema_name='UFS_RECIPIENTS')
            .has_ingest_from_file_interface()
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        delta_match_py='dbfs:/libraries/name_matching/delta_contacts.py',
        match_py='dbfs:/libraries/name_matching/match_contacts.py',
        integrated_input=intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_integrated'.format(entity)),
        ingest_input=intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_delta'.format(entity))
    )

    pre_processing = DatabricksSubmitRunOperator(
        task_id="pre_processed",
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonPreProcess",
            'parameters': ['--integratedInputFile',
                           integrated_bucket.format(date=two_day_ago, fn=entity),
                           '--deltaInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn=f'{entity}_gathered'),
                           '--deltaPreProcessedOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_pre_processed'.format(entity))]
        }
    )

    exact_match_integrated_ingested = DatabricksSubmitRunOperator(
        task_id="exact_match_integrated_ingested",
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonIntegratedExactMatch",
            'parameters': ['--integratedInputFile',
                           integrated_bucket.format(date=two_day_ago, fn=entity),
                           '--deltaInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_pre_processed'.format(entity)),
                           '--matchedExactOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_exact_matches'.format(entity)),
                           '--unmatchedIntegratedOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_integrated'.format(entity)),
                           '--unmatchedDeltaOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_delta'.format(entity))
                           ] + postgres_config
        }
    )

    join_fuzzy_matched = DatabricksSubmitRunOperator(
        task_id='join_matched',
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_fuzzy_matched_delta'.format(entity)),
                           '--entityInputFile', intermediate_bucket.format(date=one_day_ago,
                                                                           fn='{}_delta_left_overs'.format(entity)),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_delta_golden_records'.format(entity))] + postgres_config
        }
    )

    join_fuzzy_and_exact_matched = DatabricksSubmitRunOperator(
        task_id='join_fuzzy_and_exact_matched',
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombineExactAndFuzzyMatches",
            'parameters': ['--contactPersonExactMatchedInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_exact_matches'.format(entity)),
                           '--contactPersonFuzzyMatchedDeltaIntegratedInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_fuzzy_matched_delta_integrated'.format(entity)),
                           '--contactPersonsDeltaGoldenRecordsInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_delta_golden_records'.format(entity)),
                           '--contactPersonsCombinedOutputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_combined'.format(entity))]
        }
    )

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id='operators_integrated_sensor',
        external_dag_id='ohub_operators',
        external_task_id='update_golden_records'
    )

    referencing = DatabricksSubmitRunOperator(
        task_id='referencing',
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonReferencing",
            'parameters': ['--combinedInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(entity)),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_updated_references'.format(entity))]
        }
    )

    update_golden_records = DatabricksSubmitRunOperator(
        task_id='update_golden_records',
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_updated_references'.format(entity)),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn=entity)] + postgres_config
        }
    )

    ingest.last_task >> pre_processing >> exact_match_integrated_ingested
    exact_match_integrated_ingested >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> join_fuzzy_matched >> join_fuzzy_and_exact_matched >> operators_integrated_sensor
    operators_integrated_sensor >> referencing >> update_golden_records >> export.first_task
