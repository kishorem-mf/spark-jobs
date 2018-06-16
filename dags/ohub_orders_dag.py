from datetime import datetime

from airflow import DAG
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from custom_operators.empty_fallback import EmptyFallbackOperator
from custom_operators.databricks_functions import DatabricksSubmitRunOperator
from ohub_dag_config import default_args, pipeline_without_matching, databricks_conn_id, jar, \
    intermediate_bucket, one_day_ago, ingested_bucket, integrated_bucket, two_day_ago, \
    wasb_raw_container, wasb_conn_id

schema = 'orders'
clazz = 'Order'
acm_tbl = 'ORDERS'

interval = '@daily'
default_args.update(
    {'start_date': datetime(2018, 6, 14)}
)
cluster_name = "ohub_" + schema + "_{{ds}}"

with DAG('ohub_{}'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    tasks = pipeline_without_matching(
        schema=schema,
        cluster_name=cluster_name,
        clazz=clazz,
        acm_file_prefix='UFS_{}'.format(acm_tbl),
        enable_acm_delta=True,
        pars=['--orderLineFile', integrated_bucket.format(date=one_day_ago, fn='order_lines')])

    empty_fallback = EmptyFallbackOperator(
        task_id='{}_empty_fallback'.format(schema),
        container_name='prod',
        file_path=wasb_raw_container.format(date=one_day_ago, schema=schema, channel='file_interface'),
        wasb_conn_id=wasb_conn_id)

    merge = DatabricksSubmitRunOperator(
        task_id='{}_merge'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.{}Merging".format(clazz),
            'parameters': ['--orderInputFile', ingested_bucket.format(date=one_day_ago, channel='file_interface', fn=schema),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=schema),
                           '--contactPersonInputFile', integrated_bucket.format(date=one_day_ago, fn='contactpersons'),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=schema)]
        })

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id='operators_integrated_sensor',
        external_dag_id='ohub_operators',
        external_task_id='operators_update_golden_records'
    )

    contactpersons_integrated_sensor = ExternalTaskSensorOperator(
        task_id='contactpersons_integrated_sensor',
        external_dag_id='ohub_contactpersons',
        external_task_id='contact_person_update_golden_records'
    )

    order_lines_integrated_sensor = ExternalTaskSensorOperator(
        task_id='order_lines_integrated_sensor',
        external_dag_id='ohub_order_lines',
        external_task_id='order_lines_merge'
    )

    empty_fallback >> tasks['file_interface_to_parquet']
    tasks['file_interface_to_parquet'] >> operators_integrated_sensor >> merge
    tasks['file_interface_to_parquet'] >> contactpersons_integrated_sensor >> merge
    merge >> order_lines_integrated_sensor >> tasks['convert_to_acm']
