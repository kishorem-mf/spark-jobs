from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import DatabricksSubmitRunOperator
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub.ohub_dag_config import SubPipeline, DagConfig, intermediate_bucket, small_cluster_config
from ohub.ohub_dag_config import default_args, databricks_conn_id, jar, \
    one_day_ago, integrated_bucket, two_day_ago, \
    GenericPipeline

default_args.update(
    {
        'start_date': datetime(2018, 7, 26),
        'end_date': datetime(2018, 7, 27),
    }
)

orders_entity = 'orders'
orders_dag_config = DagConfig(orders_entity, is_delta=True)
orders_clazz = 'Order'

orderlines_entity = 'orderlines'
orderslines_dag_config = DagConfig(
    orderlines_entity,
    is_delta=True,
    alternate_DAG_entity='orders',
    use_alternate_entity_as_cluster=False
)
orderslines_clazz = 'OrderLine'

with DAG(orders_dag_config.dag_id, default_args=default_args, schedule_interval=orders_dag_config.schedule) as dag:
    orders = (
        GenericPipeline(orders_dag_config,
                        class_prefix=orders_clazz,
                        cluster_config=small_cluster_config(orders_dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='ORDERS',
                               extra_acm_parameters=['--orderLineFile',
                                                     integrated_bucket.format(date=one_day_ago, fn='orderlines')])
            .has_export_to_dispatcher_db(dispatcher_schema_name='ORDERS')
            .has_ingest_from_file_interface()
    )

    orderlines = (
        GenericPipeline(orderslines_dag_config,
                        class_prefix=orderslines_clazz,
                        cluster_config=small_cluster_config(orderslines_dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='ORDERLINES')
            .has_export_to_dispatcher_db(dispatcher_schema_name='ORDER_LINES')
            .has_ingest_from_file_interface(deduplicate_on_concat_id=False, alternative_schema='orders')
    )

    ingest_orders: SubPipeline = orders.construct_ingest_pipeline()
    export_orders: SubPipeline = orders.construct_export_pipeline()

    ingest_orderlines: SubPipeline = orderlines.construct_ingest_pipeline()
    export_orderlines: SubPipeline = orderlines.construct_export_pipeline()

    merge_orders = DatabricksSubmitRunOperator(
        task_id=f'{orders_entity}_merge',
        cluster_name=orders_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.{}Merging".format(orders_clazz),
            'parameters': ['--orderInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn=f'{orders_entity}_gathered'),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=orders_entity),
                           '--contactPersonInputFile', integrated_bucket.format(date=one_day_ago, fn='contactpersons'),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=orders_entity)]
        })

    merge_orderlines = DatabricksSubmitRunOperator(
        task_id=f'{orderlines_entity}_merge',
        cluster_name=orderslines_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.{}Merging".format(orderslines_clazz),
            'parameters': ['--orderLineInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn=f'{orderlines_entity}_gathered'),
                           '--previousIntegrated',
                           integrated_bucket.format(date=two_day_ago, fn=orderslines_dag_config.entity),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=orderslines_dag_config.entity)]
        })

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id='operators_integrated_sensor',
        external_dag_id='ohub_operators',
        external_task_id='update_golden_records'
    )

    contactpersons_integrated_sensor = ExternalTaskSensorOperator(
        task_id='contactpersons_integrated_sensor',
        external_dag_id='ohub_contactpersons',
        external_task_id='update_golden_records'
    )

    operators_integrated_sensor >> ingest_orders.first_task
    contactpersons_integrated_sensor >> ingest_orders.first_task
    ''' 
    -------------
    orderlines is independent from orders, but it MUST have the emtpy fallback operator from orders to be completed
    to be able to run since they have the same root raw files.
    However, the BashOperator (empty fallback) should always complete before the CreateCluster has run, so this
    dependency should not have to be explicitly given in airflow
    '''
    operators_integrated_sensor >> ingest_orderlines.first_task
    contactpersons_integrated_sensor >> ingest_orderlines.first_task
    '''
    -------------
    '''

    ingest_orders.last_task >> merge_orders >> export_orders.first_task
    merge_orderlines >> export_orders.first_task
    ingest_orderlines.last_task >> merge_orderlines >> export_orderlines.first_task
    ingest_orders.first_task >> export_orders.last_task
    ingest_orders.first_task >> export_orderlines.last_task
    ingest_orderlines.first_task >> export_orders.last_task
    ingest_orderlines.first_task >> export_orderlines.last_task
