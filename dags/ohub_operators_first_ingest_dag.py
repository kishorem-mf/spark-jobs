from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from ohub_dag_config import \
    default_args, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, \
    postgres_config, GenericPipeline, SubPipeline

default_args.update(
    {
        'start_date': datetime(2018, 6, 3),
        'pool': 'ohub_operators_pool'
    }
)
interval = '@once'
schema = 'operators'
cluster_name = "ohub_operators_initial_load_{{ds}}"

with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    generic = (
        GenericPipeline(schema=schema, cluster_name=cluster_name, clazz='Operator')
            .has_export_to_acm(acm_schema_name='UFS_OPERATORS')
            .has_ingest_from_file_interface()
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        match_py='dbfs:/libraries/name_matching/match_operators.py',
        ingest_input=ingested_bucket.format(date='{{ds}}', fn=schema, channel='*')
    )

    merge_operators = DatabricksSubmitRunOperator(
        task_id='{}_join'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date='{{ds}}', fn='{}_matched'.format(schema)),
                           '--entityInputFile', ingested_bucket.format(date='{{ds}}', fn=schema, channel='*'),
                           '--outputFile', integrated_bucket.format(date='{{ds}}', fn=schema)] + postgres_config
        }
    )

    ingest.last_task >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> merge_operators >> export.first_task
