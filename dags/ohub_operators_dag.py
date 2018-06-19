from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from ohub_dag_config import \
    default_args, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, interval, \
    one_day_ago, two_day_ago, postgres_config, \
    GenericPipeline, SubPipeline

default_args.update(
    {
        'start_date': datetime(2018, 6, 4),
        'pool': 'ohub_operators_pool'
    }
)
schema = 'operators'
cluster_name = "ohub_operators_{{ds}}"

with DAG('ohub_{}'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    generic = (
        GenericPipeline(schema=schema, cluster_name=cluster_name, clazz='Operator')
            .is_delta()
            .has_export_to_acm(acm_schema_name='UFS_OPERATORS')
            .has_ingest_from_file_interface()
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        ingest_input=ingested_bucket.format(date=one_day_ago, fn=schema, channel='*'),
        match_py='dbfs:/libraries/name_matching/match_operators.py',
        integrated_input=integrated_bucket.format(date=two_day_ago, fn=schema),
        delta_match_py='dbfs:/libraries/name_matching/delta_operators.py',
    )

    join_operators = DatabricksSubmitRunOperator(
        task_id='{}_merge'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_fuzzy_matched_delta'.format(schema)),
                           '--entityInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_delta_left_overs'.format(schema)),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_delta_golden_records'.format(schema))] + postgres_config
        })

    combine_to_create_integrated = DatabricksSubmitRunOperator(
        task_id='{}_combine_to_create_integrated'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.OperatorCombining",
            'parameters': ['--integratedUpdated',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_fuzzy_matched_delta_integrated'.format(schema)),
                           '--newGolden',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_delta_golden_records'.format(schema)),
                           '--combinedEntities',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(schema))]
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
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(schema)),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn=schema)] + postgres_config
        }
    )

    update_operators_table = DatabricksSubmitRunOperator(
        task_id='{}_update_table'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        notebook_task={'notebook_path': '/Users/tim.vancann@unilever.com/update_integrated_tables'}
    )

    ingest.last_task >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> join_operators >> combine_to_create_integrated >> update_golden_records
    update_golden_records >> update_operators_table
    update_golden_records >> export.first_task
