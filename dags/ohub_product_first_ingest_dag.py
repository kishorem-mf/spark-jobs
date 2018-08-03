from datetime import datetime

from airflow import DAG

from dags import config
from dags.config import small_cluster_config
from ohub.operators.wasb_operator import WasbCopyOperator
from ohub.utils.airflow import SubPipeline, DagConfig, GenericPipeline

dag_args = {**config.dag_default_args, **{
        "start_date": datetime(2018, 7, 25),
        "end_date": datetime(2018, 7, 25),
    }}

entity = "products"
dag_config = DagConfig(entity, is_delta=False)
clazz = "Product"

with DAG(
    dag_config.dag_id, default_args=dag_args, schedule_interval=dag_config.schedule
) as dag:
    generic = (
        GenericPipeline(
            dag_config,
            class_prefix=clazz,
            cluster_config=small_cluster_config(dag_config.cluster_name),
            databricks_conn_id=config.databricks_conn_id,
            spark_jobs_jar=config.spark_jobs_jar,
            wasb_raw_container=config.wasb_raw_container,
            wasb_conn_id=config.wasb_conn_id,
            ingested_bucket=config.ingested_bucket,
            intermediate_bucket=config.intermediate_bucket,
        )
        .has_export_to_acm(
            acm_schema_name="PRODUCTS",
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
            container_name=config.container_name,
            wasb_export_container=config.wasb_export_container,
        )
        .has_export_to_dispatcher_db(
            dispatcher_schema_name="ORDER_PRODUCTS",
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
        )
        .has_ingest_from_file_interface(raw_bucket=config.raw_bucket)
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()

    # We need this CopyOperator for one reason:
    # The end of the pipeline should always produce an integrated file, since Products has only an ingest step the
    # result of the ingest is copied to the integrated 'folder'
    copy = WasbCopyOperator(
        task_id="copy_to_integrated",
        wasb_conn_id="azure_blob",
        container_name="prod",
        blob_name=config.wasb_integrated_container.format(date="{{ ds }}", fn=entity),
        copy_source=config.http_intermediate_container.format(
            storage_account="ulohub2storedevne",
            container="prod",
            date="{{ ds }}",
            fn=f"{entity}_gathered",
        ),
    )

    ingest.last_task >> copy >> export.first_task
