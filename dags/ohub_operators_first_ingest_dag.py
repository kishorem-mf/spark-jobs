from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from ohub_dag_config import \
    default_args, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, \
    create_cluster, terminate_cluster, default_cluster_config, \
    postgres_config, ingest_task, fuzzy_matching_tasks, acm_convert_and_move, GenericPipeline

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

    operators_file_interface_to_parquet = ingest_task(
        schema=schema,
        channel='file_interface',
        clazz="com.unilever.ohub.spark.tsv2parquet.file_interface.OperatorConverter",
        field_separator=u"\u2030",
        cluster_name=cluster_name
    )

    begin_fuzzy_matching = BashOperator(
        task_id='{}_start_fuzzy_matching'.format(schema),
        bash_command='echo "start fuzzy matching"',
    )

    matching_tasks = fuzzy_matching_tasks(
        schema=schema,
        cluster_name=cluster_name,
        match_py='dbfs:/libraries/name_matching/match_operators.py',
        ingested_input=ingested_bucket.format(date='{{ds}}', fn=schema, channel='*')
    )

    end_fuzzy_matching = BashOperator(
        task_id='{}_end_fuzzy_matching'.format(schema),
        bash_command='echo "end fuzzy matching"',
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

    convert_to_acm = acm_convert_and_move(
        schema=schema,
        cluster_name=cluster_name,
        clazz='ContactPerson',
        acm_file_prefix='UFS_OPERATORS',
        send_postgres_config=True
    )

    cluster_up >> operators_file_interface_to_parquet >> begin_fuzzy_matching
    for t in matching_tasks:
        begin_fuzzy_matching >> t >> end_fuzzy_matching
    end_fuzzy_matching >> merge_operators >> convert_to_acm >> cluster_down
