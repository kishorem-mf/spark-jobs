from datetime import datetime

from airflow import DAG

from dags import config
from dags.config import start_date_delta
from ohub.utils.airflow import DagConfig, GenericPipeline, SubPipeline
from ohub.operators.databricks_operator import DatabricksSubmitRunOperator
from ohub.operators.external_task_sensor_operator import ExternalTaskSensorOperator

dag_args = {
    **config.dag_default_args,
    **{
        "start_date": start_date_delta,
    },
}

entity = "subscriptions"
dag_config = DagConfig(entity, is_delta=True)

with DAG(
    dag_config.dag_id, default_args=dag_args, schedule_interval=dag_config.schedule
) as dag:
    cluster_conf = config.cluster_config(dag_config.cluster_name)

    generic = (
        GenericPipeline(
            dag_config,
            class_prefix="Subscription",
            cluster_config=cluster_conf,
            databricks_conn_id=config.databricks_conn_id,
            ingested_bucket=config.ingested_bucket,
            intermediate_bucket=config.intermediate_bucket,
            integrated_bucket=config.integrated_bucket,
            spark_jobs_jar=config.spark_jobs_jar,
            wasb_conn_id=config.wasb_conn_id,
            wasb_raw_container=config.wasb_raw_container,
        )
        .has_common_ingest(raw_bucket=config.raw_bucket)
        # TODO remove these once the common ingest is working properly
        .has_ingest_from_web_event(
            raw_bucket=config.raw_bucket, ingested_bucket=config.ingested_bucket
        )
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()

    merge = DatabricksSubmitRunOperator(
        task_id=f"{entity}_merge",
        cluster_name=cluster_conf['cluster_name'],
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.SubscriptionMerging",
            "parameters": [
                "--subscriptionInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn=f"{entity}_pre_processed"
                ),
                "--previousIntegrated",
                config.integrated_bucket.format(
                    date="{{ yesterday_ds }}", fn=entity
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
        external_dag_id="ohub_contactpersons",
        external_task_id="update_golden_records",
    )

    contactpersons_integrated_sensor >> ingest.first_task
    ingest.last_task >> merge
    ingest.first_task >> merge
