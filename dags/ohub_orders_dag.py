from datetime import datetime

from airflow import DAG

from dags import config
from ohub.operators.databricks_operator import DatabricksSubmitRunOperator
from ohub.operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub.utils.airflow import DagConfig, GenericPipeline, SubPipeline

dag_args = {**config.dag_default_args, **{"start_date": datetime(2018, 6, 26)}}

orders_dag_config = DagConfig("orders", is_delta=True)
orderslines_dag_config = DagConfig(
    "orderlines",
    is_delta=True,
    alternate_dag_entity="orders",
    use_alternate_entity_as_cluster=False,
)

with DAG(
    orders_dag_config.dag_id,
    default_args=dag_args,
    schedule_interval=orders_dag_config.schedule,
) as dag:
    orders = (
        GenericPipeline(
            orders_dag_config,
            class_prefix=config.ohub_entities["orders"]["spark_class"],
            cluster_config=config.small_cluster_config(orders_dag_config.cluster_name),
            databricks_conn_id=config.databricks_conn_id,
            ingested_bucket=config.ingested_bucket,
            intermediate_bucket=config.intermediate_bucket,
            spark_jobs_jar=config.spark_jobs_jar,
            wasb_conn_id=config.wasb_conn_id,
            wasb_raw_container=config.wasb_raw_container,
        )
        .has_export_to_acm(
            acm_schema_name="ORDERS",
            extra_acm_parameters=[
                "--orderLineFile",
                config.integrated_bucket.format(date="{{ ds }}", fn="orderlines"),
            ],
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
            postgres_conn_id="postgres_channels",
            container_name=config.container_name,
            wasb_export_container=config.wasb_export_container,
        )
        .has_export_to_dispatcher_db(
            dispatcher_schema_name="ORDERS",
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
            postgres_conn_id="postgres_channels",
        )
        .has_ingest_from_file_interface(
            raw_bucket=config.raw_bucket, postgres_conn_id="postgres_channels"
        )
    )

    orderlines = (
        GenericPipeline(
            orderslines_dag_config,
            class_prefix=config.ohub_entities["orderlines"]["spark_class"],
            cluster_config=config.small_cluster_config(
                orderslines_dag_config.cluster_name
            ),
            databricks_conn_id=config.databricks_conn_id,
            ingested_bucket=config.ingested_bucket,
            intermediate_bucket=config.intermediate_bucket,
            spark_jobs_jar=config.spark_jobs_jar,
            wasb_conn_id=config.wasb_conn_id,
            wasb_raw_container=config.wasb_raw_container,
        )
        .has_export_to_acm(
            acm_schema_name="ORDERLINES",
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
            postgres_conn_id="postgres_channels",
            container_name=config.container_name,
            wasb_export_container=config.wasb_export_container,
        )
        .has_export_to_dispatcher_db(
            dispatcher_schema_name="ORDER_LINES",
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
            postgres_conn_id="postgres_channels",
        )
        .has_ingest_from_file_interface(
            raw_bucket=config.raw_bucket,
            postgres_conn_id="postgres_channels",
            deduplicate_on_concat_id=False,
            alternative_schema="orders",
        )
    )

    ingest_orders: SubPipeline = orders.construct_ingest_pipeline()
    export_orders: SubPipeline = orders.construct_export_pipeline()

    ingest_orderlines: SubPipeline = orderlines.construct_ingest_pipeline()
    export_orderlines: SubPipeline = orderlines.construct_export_pipeline()

    merge_orders = DatabricksSubmitRunOperator(
        task_id="orders_merge",
        cluster_name=orders_dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.{}Merging".format(
                config.ohub_entities["orders"]["spark_class"]
            ),
            "parameters": [
                "--orderInputFile",
                config.intermediate_bucket.format(date="{{ ds }}", fn="orders_gathered"),
                "--previousIntegrated",
                config.integrated_bucket.format(date="{{ yesterday_ds }}", fn="orders"),
                "--contactPersonInputFile",
                config.integrated_bucket.format(date="{{ ds }}", fn="contactpersons"),
                "--operatorInputFile",
                config.integrated_bucket.format(date="{{ ds }}", fn="operators"),
                "--outputFile",
                config.integrated_bucket.format(date="{{ ds }}", fn="orders"),
            ],
        },
    )

    merge_orderlines = DatabricksSubmitRunOperator(
        task_id="orderlines_merge",
        cluster_name=orderslines_dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.{}Merging".format(
                config.ohub_entities["orderlines"]["spark_class"]
            ),
            "parameters": [
                "--orderLineInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="orderlines_gathered"
                ),
                "--previousIntegrated",
                config.integrated_bucket.format(
                    date="{{ yesterday_ds }}", fn=orderslines_dag_config.entity
                ),
                "--outputFile",
                config.integrated_bucket.format(
                    date="{{ ds }}", fn=orderslines_dag_config.entity
                ),
            ],
        },
    )

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id="operators_integrated_sensor",
        external_dag_id="ohub_operators",
        external_task_id="update_golden_records",
    )

    contactpersons_integrated_sensor = ExternalTaskSensorOperator(
        task_id="contactpersons_integrated_sensor",
        external_dag_id="ohub_contactpersons",
        external_task_id="update_golden_records",
    )

    operators_integrated_sensor >> ingest_orders.first_task
    contactpersons_integrated_sensor >> ingest_orders.first_task
    """
    -------------
    orderlines is independent from orders, but it MUST have the emtpy fallback operator from orders to be completed
    to be able to run since they have the same root raw files.
    However, the BashOperator (empty fallback) should always complete before the CreateCluster has run, so this
    dependency should not have to be explicitly given in airflow
    """
    operators_integrated_sensor >> ingest_orderlines.first_task
    contactpersons_integrated_sensor >> ingest_orderlines.first_task
    """
    -------------
    """

    ingest_orders.last_task >> merge_orders >> export_orders.first_task
    merge_orderlines >> export_orders.first_task
    ingest_orderlines.last_task >> merge_orderlines >> export_orderlines.first_task
