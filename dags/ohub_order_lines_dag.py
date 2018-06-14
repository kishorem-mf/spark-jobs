from datetime import datetime

from airflow import DAG
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from custom_operators.databricks_functions import DatabricksSubmitRunOperator
from ohub_dag_config import default_args, pipeline_without_matching, databricks_conn_id, jar, \
    intermediate_bucket, one_day_ago, ingested_bucket, integrated_bucket, two_day_ago

schema = 'order_lines'
clazz = 'OrderLine'
acm_tbl = 'ORDERLINES'

interval = '@daily'
default_args.update(
    {'start_date': datetime(2018, 6, 3)}
)
cluster_name = "ohub_" + schema + "_{{ds}}"

with DAG('ohub_{}'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    tasks = pipeline_without_matching(
        schema=schema,
        cluster_name=cluster_name,
        clazz=clazz,
        acm_file_prefix='UFS_{}'.format(acm_tbl),
        enable_acm_delta=True)

    merge = DatabricksSubmitRunOperator(
        task_id='{}_merge'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.{}Merging".format(clazz),
            'parameters': ['--orderLineInputFile'.format(schema), ingested_bucket.format(date=one_day_ago, channel='file_interface', fn=schema),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=schema),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=schema)]
        })

    products_integrated_sensor = ExternalTaskSensorOperator(
        task_id='products_integrated_sensor',
        external_dag_id='ohub_products',
        external_task_id='products_merge'
    )

    orders_integrated_sensor = ExternalTaskSensorOperator(
        task_id='orders_integrated_sensor',
        external_dag_id='ohub_orders',
        external_task_id='orders_merge'
    )

    tasks['file_interface_to_parquet'] >> products_integrated_sensor >> orders_integrated_sensor >> merge >> tasks['convert_to_acm']
