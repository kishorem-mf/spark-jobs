from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import DatabricksSubmitRunOperator
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub_dag_config import default_args, pipeline_without_matching, databricks_conn_id, jar, \
    one_day_ago, ingested_bucket, integrated_bucket

schema = 'orders'
clazz = 'Order'

interval = '@once'
default_args.update(
    {'start_date': datetime(2018, 6, 13)}
)
cluster_name = "ohub_orders_initial_load_{{ds}}"

with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    tasks = pipeline_without_matching(
        schema=schema,
        cluster_name=cluster_name,
        clazz=clazz,
        acm_file_prefix='UFS_ORDERS',
        pars=['--orderLineFile', integrated_bucket.format(date=one_day_ago, fn='orderlines')])

    merge = DatabricksSubmitRunOperator(
        task_id='{}_merge'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.{}Merging".format(clazz),
            'parameters': ['--orderInputFile',
                           ingested_bucket.format(date=one_day_ago, channel='file_interface', fn=schema),
                           '--contactPersonInputFile', integrated_bucket.format(date=one_day_ago, fn='contactpersons'),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=schema)]
        })

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id='operators_integrated_sensor',
        external_dag_id='ohub_operators_first_ingest',
        external_task_id='operators_join'
    )

    contactpersons_integrated_sensor = ExternalTaskSensorOperator(
        task_id='contactpersons_integrated_sensor',
        external_dag_id='ohub_contactpersons_first_ingest',
        external_task_id='contact_person_update_golden_records'
    )

    tasks['file_interface_to_parquet'] >> operators_integrated_sensor >> merge
    tasks['file_interface_to_parquet'] >> contactpersons_integrated_sensor >> merge
    merge >> tasks['convert_to_acm']
