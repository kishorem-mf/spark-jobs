from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import DatabricksSubmitRunOperator
from ohub_dag_config import default_args, databricks_conn_id, jar, \
    one_day_ago, ingested_bucket, integrated_bucket, two_day_ago, \
    GenericPipeline, SubPipeline

schema = 'orderlines'
clazz = 'OrderLine'
acm_tbl = 'ORDERLINES'

interval = '@daily'
default_args.update(
    {'start_date': datetime(2018, 6, 14)}
)
cluster_name = "ohub_" + schema + "_{{ds}}"

with DAG('ohub_{}'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:

    generic = (
        GenericPipeline(schema=schema, cluster_name=cluster_name, clazz=clazz)
            .is_delta()
            .has_export_to_acm(acm_schema_name='UFS_ORDERSLINES')
            .has_ingest_from_file_interface(alternative_schema='orders')
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()

    merge = DatabricksSubmitRunOperator(
        task_id='{}_merge'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.{}Merging".format(clazz),
            'parameters': ['--orderLineInputFile', ingested_bucket.format(date=one_day_ago, channel='file_interface', fn=schema),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=schema),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=schema)]
        })

    ingest.last_task >> merge >> export.first_task
