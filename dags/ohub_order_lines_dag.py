from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import DatabricksSubmitRunOperator
from ohub_dag_config import default_args, databricks_conn_id, jar, \
    one_day_ago, ingested_bucket, integrated_bucket, two_day_ago, \
    GenericPipeline, SubPipeline, DagConfig

default_args.update(
    {'start_date': datetime(2018, 6, 14)}
)

entity = 'orderlines'
dag_config = DagConfig(entity, is_delta=True)
clazz = 'OrderLine'

with DAG(dag_config.dag_id, default_args=default_args, schedule_interval=dag_config.schedule) as dag:
    generic = (
        GenericPipeline(dag_config, class_prefix=clazz)
            .has_export_to_acm(acm_schema_name='UFS_ORDERLINES')
            .has_ingest_from_file_interface(deduplicate_on_concat_id=False, alternative_schema='orders')
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()

    merge = DatabricksSubmitRunOperator(
        task_id='merge',
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.{}Merging".format(clazz),
            'parameters': ['--orderLineInputFile',
                           ingested_bucket.format(date=one_day_ago, channel='file_interface', fn=entity),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=entity),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=entity)]
        })

    ingest.last_task >> merge >> export.first_task
