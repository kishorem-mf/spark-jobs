from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from ohub.ohub_dag_config import \
    default_args, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, \
    postgres_config, GenericPipeline, SubPipeline, one_day_ago, DagConfig, large_cluster_config

default_args.update(
    {
        'start_date': datetime(2018, 6, 3),
        'pool': 'ohub_operators_pool'
    }
)

entity = 'operators'
dag_config = DagConfig(entity, is_delta=False)

with DAG(dag_config.dag_id, default_args=default_args, schedule_interval=dag_config.schedule) as dag:
    generic = (
        GenericPipeline(dag_config,
                        class_prefix='Operator',
                        cluster_config=large_cluster_config(dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='OPERATORS')
            .has_export_to_dispatcher_db(dispatcher_schema_name='OPERATORS')
            .has_ingest_from_file_interface()
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        match_py='dbfs:/libraries/name_matching/match_operators.py',
        ingest_input=intermediate_bucket.format(date=one_day_ago, fn=f'{entity}_gathered'),
    )

    join = DatabricksSubmitRunOperator(
        task_id='join',
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_matched'.format(entity)),
                           '--entityInputFile', ingested_bucket.format(date=one_day_ago, fn=entity, channel='*'),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=entity)] + postgres_config
        }
    )

    ingest.last_task >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> join >> export.first_task
