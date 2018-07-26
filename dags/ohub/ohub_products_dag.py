from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import DatabricksSubmitRunOperator
from ohub.ohub_dag_config import default_args, databricks_conn_id, jar, \
    one_day_ago, integrated_bucket, two_day_ago, \
    GenericPipeline, SubPipeline, DagConfig, intermediate_bucket, small_cluster_config

default_args.update(
    {'start_date': datetime(2018, 6, 14)}
)

entity = 'products'
dag_config = DagConfig(entity, is_delta=True)
clazz = 'Product'

with DAG(dag_config.dag_id, default_args=default_args, schedule_interval=dag_config.schedule) as dag:
    generic = (
        GenericPipeline(dag_config,
                        class_prefix=clazz,
                        cluster_config=small_cluster_config(dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='PRODUCTS')
            .has_export_to_dispatcher_db(dispatcher_schema_name='ORDER_PRODUCTS')
            .has_ingest_from_file_interface()
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
            'parameters': ['--productsInputFile', intermediate_bucket.format(date=one_day_ago, fn=f'{entity}_gathered'),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=entity),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=entity)]
        })

    ingest.last_task >> merge >> export.first_task
    ingest.first_task >> export.last_task
