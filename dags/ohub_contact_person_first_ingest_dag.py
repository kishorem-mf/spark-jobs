from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub_dag_config import \
    default_args, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, postgres_config, \
    one_day_ago, GenericPipeline, SubPipeline

default_args.update(
    {
        'start_date': datetime(2018, 6, 3),
        'pool': 'ohub_contactpersons_pool'
    }
)
interval = '@once'

schema = 'contactpersons'
cluster_name = "ohub_contactpersons_initial_load_{{ds}}"

with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    generic = (
        GenericPipeline(schema=schema, cluster_name=cluster_name, clazz='ContactPerson')
            .has_export_to_acm(acm_schema_name='UFS_RECIPIENTS')
            .has_ingest_from_file_interface()
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        match_py='dbfs:/libraries/name_matching/match_contacts.py',
        ingest_input=intermediate_bucket.format(date='{{ds}}', fn='{}_left_overs'.format(schema)),
    )

    exact_match = DatabricksSubmitRunOperator(
        task_id="{}_exact_match".format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonExactMatcher",
            'parameters': ['--inputFile', ingested_bucket.format(date='{{ds}}',
                                                                 fn=schema,
                                                                 channel='file_interface'),
                           '--exactMatchOutputFile', intermediate_bucket.format(date='{{ds}}',
                                                                                fn='{}_exact_matches'.format(schema)),
                           '--leftOversOutputFile', intermediate_bucket.format(date='{{ds}}',
                                                                               fn='{}_left_overs'.format(
                                                                                   schema))] + postgres_config
        }
    )

    join = DatabricksSubmitRunOperator(
        task_id='{}_merge'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date='{{ds}}', fn='{}_matched'.format(schema)),
                           '--entityInputFile', intermediate_bucket.format(date='{{ds}}',
                                                                           fn='{}_left_overs'.format(schema)),
                           '--outputFile', intermediate_bucket.format(date='{{ds}}', fn=schema)] + postgres_config
        }
    )

    combine = DatabricksSubmitRunOperator(
        task_id='{}_combining'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],

        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombining",
            'parameters': ['--integratedUpdated', intermediate_bucket.format(date='{{ds}}',
                                                                             fn='{}_exact_matches'.format(schema)),
                           '--newGolden', intermediate_bucket.format(date='{{ds}}',
                                                                     fn=schema,
                                                                     channel='*'),
                           '--combinedEntities', intermediate_bucket.format(date='{{ds}}',
                                                                            fn='{}_combined'.format(schema))]
        }
    )

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id='operators_integrated_sensor',
        external_dag_id='ohub_operators_first_ingest',
        external_task_id='operators_join'
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
            'parameters': ['--combinedInputFile', intermediate_bucket.format(date='{{ds}}',
                                                                             fn='{}_combined'.format(schema)),
                           '--operatorInputFile', integrated_bucket.format(date='{{ds}}',
                                                                           fn='operators',
                                                                           channel='*'),
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

    ingest.last_task >> exact_match >> fuzzy_matching.first_task >> join >> combine
    combine >> operators_integrated_sensor >> referencing >> update_golden_records >> export.first_task
