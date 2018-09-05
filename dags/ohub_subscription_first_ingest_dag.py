from datetime import datetime

from airflow import DAG

from dags import config
from dags.config import start_date_first
from ohub.utils.airflow import DagConfig, GenericPipeline, SubPipeline
from ohub.operators.databricks_operator import DatabricksSubmitRunOperator
from ohub.operators.external_task_sensor_operator import ExternalTaskSensorOperator

dag_args = {
    **config.dag_default_args,
    **{
        "start_date": start_date_first,
    },
}

entity = "subscriptions"
dag_config = DagConfig(entity, is_delta=False)

with DAG(
    dag_config.dag_id, default_args=dag_args, schedule_interval=dag_config.schedule
) as dag:
    generic = (
        GenericPipeline(
            dag_config,
            class_prefix="Subscription",
            cluster_config=config.cluster_config(dag_config.cluster_name),
            databricks_conn_id=config.databricks_conn_id,
            ingested_bucket=config.ingested_bucket,
            intermediate_bucket=config.intermediate_bucket,
            spark_jobs_jar=config.spark_jobs_jar,
            wasb_conn_id=config.wasb_conn_id,
            wasb_raw_container=config.wasb_raw_container,
        )
        .has_ingest_from_file_interface(raw_bucket=config.raw_bucket)
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()

    merge = DatabricksSubmitRunOperator(
        task_id=f"{entity}_merge",
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.SubscriptionMerging",
            "parameters": [
                "--subscriptionInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn=f"{entity}_gathered"
                ),
                "--contactPersonInputFile",
                config.integrated_bucket.format(date="{{ ds }}", fn="contactpersons"),
                "--outputFile",
                config.integrated_bucket.format(date="{{ ds }}", fn=entity),
            ],
        },
    )

    contactpersons_integrated_sensor = ExternalTaskSensorOperator(
        task_id="contactpersons_integrated_sensor",
        external_dag_id="ohub_contactpersons_initial_load",
        external_task_id="update_golden_records",
    )

    contactpersons_integrated_sensor >> ingest.first_task
    ingest.last_task >> merge
