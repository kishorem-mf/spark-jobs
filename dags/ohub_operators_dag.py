from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from ohub.ohub_dag_config import \
    default_args, databricks_conn_id, jar, intermediate_bucket, integrated_bucket, one_day_ago, \
    two_day_ago, postgres_config, \
    GenericPipeline, SubPipeline, DagConfig, large_cluster_config

default_args.update(
    {
        'start_date': datetime(2018, 6, 4),
        'pool': 'ohub_operators_pool'
    }
)
entity = 'operators'
dag_config = DagConfig(entity, is_delta=True)

with DAG(dag_config.dag_id, default_args=default_args, schedule_interval=dag_config.schedule) as dag:
    generic = (
        GenericPipeline(dag_config,
                        class_prefix='Operator',
                        cluster_config=large_cluster_config(dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='OPERATORS')
            .has_export_to_dispatcher_db(dispatcher_schema_name='OPERATORS')
            .has_ingest_from_file_interface()
            .has_ingest_from_web_event()
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        ingest_input=intermediate_bucket.format(date=one_day_ago, fn=f'{entity}_gathered'),
        match_py='dbfs:/libraries/name_matching/match_operators.py',
        integrated_input=integrated_bucket.format(date=two_day_ago, fn=dag_config.entity),
        delta_match_py='dbfs:/libraries/name_matching/delta_operators.py',
    )

    join = DatabricksSubmitRunOperator(
        task_id='merge',
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_fuzzy_matched_delta'.format(entity)),
                           '--entityInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_delta_left_overs'.format(entity)),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_delta_golden_records'.format(entity))] + postgres_config
        })

    combine_to_create_integrated = DatabricksSubmitRunOperator(
        task_id='combine_to_create_integrated',
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.OperatorCombining",
            'parameters': ['--integratedUpdated',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_fuzzy_matched_delta_integrated'.format(entity)),
                           '--newGolden',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_delta_golden_records'.format(entity)),
                           '--combinedEntities',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(entity))]
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
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(entity)),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn=entity)] + postgres_config
        }
    )

    update_operators_table = DatabricksSubmitRunOperator(
        task_id='update_table',
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        notebook_task={'notebook_path': '/Users/tim.vancann@unilever.com/update_integrated_tables'}
    )

    ingest.last_task >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> join >> combine_to_create_integrated >> update_golden_records
    update_golden_records >> update_operators_table
    update_golden_records >> export.first_task
