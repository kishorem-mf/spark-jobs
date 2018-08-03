from datetime import datetime

from airflow import DAG

from dags import config
from dags.config import small_cluster_config, start_date_delta
from ohub.operators.databricks_operator import DatabricksSubmitRunOperator
from ohub.utils.airflow import DagConfig, GenericPipeline, SubPipeline

dag_args = {**config.dag_default_args, **{
        'start_date': start_date_delta,
    }}

entity = "products"
dag_config = DagConfig(entity, is_delta=True)
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

    merge = DatabricksSubmitRunOperator(
        task_id="merge",
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.{}Merging".format(clazz),
            "parameters": [
                "--productsInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn=f"{entity}_gathered"
                ),
                "--previousIntegrated",
                config.integrated_bucket.format(date="{{ yesterday_ds }}", fn=entity),
                "--outputFile",
                config.integrated_bucket.format(date="{{ ds }}", fn=entity),
            ],
        },
    )

    ingest.last_task >> merge >> export.first_task
