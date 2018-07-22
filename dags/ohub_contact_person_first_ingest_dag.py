from datetime import datetime

from airflow import DAG

from ohub.operators.databricks_operator import DatabricksSubmitRunOperator
from ohub.operators.external_task_sensor_operator import ExternalTaskSensorOperator
from dags.ohub_dag_config import \
    default_args, databricks_conn_id, jar, intermediate_bucket, integrated_bucket, postgres_config, \
    one_day_ago, GenericPipeline, DagConfig, large_cluster_config
from ohub.utils.airflow import SubPipeline


default_args.update(
    {
        'start_date': datetime(2018, 6, 3),
        'pool': 'ohub_contactpersons_pool'
    }
)

entity = 'contactpersons'
dag_config = DagConfig(entity, is_delta=False)

with DAG(dag_config.dag_id, default_args=default_args, schedule_interval=dag_config.schedule) as dag:
    generic = (
        GenericPipeline(dag_config,
                        class_prefix='ContactPerson',
                        cluster_config=large_cluster_config(dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='RECIPIENTS')
            .has_export_to_dispatcher_db(dispatcher_schema_name='CONTACT_PERSONS')
            .has_ingest_from_file_interface()
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        match_py='dbfs:/libraries/name_matching/match_contacts.py',
        ingest_input=intermediate_bucket.format(date='{{ds}}', fn='{}_left_overs'.format(entity)),
    )

    exact_match = DatabricksSubmitRunOperator(
        task_id="{}_exact_match".format(entity),
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonExactMatcher",
            'parameters': ['--inputFile', intermediate_bucket.format(date=one_day_ago, fn=f'{entity}_gathered'),
                           '--exactMatchOutputFile', intermediate_bucket.format(date=one_day_ago,
                                                                                fn='{}_exact_matches'.format(entity)),
                           '--leftOversOutputFile', intermediate_bucket.format(date=one_day_ago,
                                                                               fn='{}_left_overs'.format(
                                                                                   entity))] + postgres_config
        }
    )

    join = DatabricksSubmitRunOperator(
        task_id='{}_merge'.format(entity),
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_matched'.format(entity)),
                           '--entityInputFile', intermediate_bucket.format(date=one_day_ago,
                                                                           fn='{}_left_overs'.format(entity)),
                           '--outputFile', intermediate_bucket.format(date=one_day_ago, fn=entity)] + postgres_config
        }
    )

    combine = DatabricksSubmitRunOperator(
        task_id='{}_combining'.format(entity),
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],

        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombining",
            'parameters': ['--integratedUpdated', intermediate_bucket.format(date=one_day_ago,
                                                                             fn='{}_exact_matches'.format(entity)),
                           '--newGolden', intermediate_bucket.format(date=one_day_ago,
                                                                     fn=entity,
                                                                     channel='*'),
                           '--combinedEntities', intermediate_bucket.format(date=one_day_ago,
                                                                            fn='{}_combined'.format(entity))]
        }
    )

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id='operators_integrated_sensor',
        external_dag_id='ohub_operators_initial_load',
        external_task_id='join'
    )

    referencing = DatabricksSubmitRunOperator(
        task_id='{}_referencing'.format(entity),
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],

        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonReferencing",
            'parameters': ['--combinedInputFile', intermediate_bucket.format(date=one_day_ago,
                                                                             fn='{}_combined'.format(entity)),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago,
                                                                           fn='operators',
                                                                           channel='*'),
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

    ingest.last_task >> exact_match >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> join >> combine >> operators_integrated_sensor >> referencing >> update_golden_records >> export.first_task
