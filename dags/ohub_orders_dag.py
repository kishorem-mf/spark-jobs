from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import DatabricksSubmitRunOperator
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub_dag_config import SubPipeline, DagConfig
from ohub_dag_config import default_args, databricks_conn_id, jar, \
    one_day_ago, ingested_bucket, integrated_bucket, two_day_ago, \
    GenericPipeline

default_args.update(
    {'start_date': datetime(2018, 6, 14)}
)

entity = 'orders'
dag_config = DagConfig(entity, is_delta=True)
clazz = 'Order'

with DAG(dag_config.dag_id, default_args=default_args, schedule_interval=dag_config.schedule) as dag:
    generic = (
        GenericPipeline(dag_config, class_prefix=clazz)
            .has_export_to_acm(acm_schema_name='UFS_ORDERS',
                               extra_acm_parameters=['--orderLineFile',
                                                     integrated_bucket.format(date=one_day_ago, fn='orderlines')])
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
            'parameters': ['--orderInputFile',
                           ingested_bucket.format(date=one_day_ago, channel='file_interface', fn=entity),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=entity),
                           '--contactPersonInputFile', integrated_bucket.format(date=one_day_ago, fn='contactpersons'),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=entity)]
        })

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id='operators_integrated_sensor',
        external_dag_id='ohub_operators',
        external_task_id='operators_update_golden_records'
    )

    contactpersons_integrated_sensor = ExternalTaskSensorOperator(
        task_id='contactpersons_integrated_sensor',
        external_dag_id='ohub_contactpersons',
        external_task_id='contactpersons_update_golden_records'
    )

    order_lines_integrated_sensor = ExternalTaskSensorOperator(
        task_id='orderlines_integrated_sensor',
        external_dag_id='ohub_orderlines',
        external_task_id='orderlines_merge'
    )

    ingest.last_task >> operators_integrated_sensor >> merge
    ingest.last_task >> contactpersons_integrated_sensor >> merge
    merge >> order_lines_integrated_sensor >> export.first_task
