from datetime import datetime

from airflow import DAG

from dags import config
from dags.config import start_date_first
from ohub.utils.airflow import DagConfig, GenericPipeline, SubPipeline

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
            cluster_config=config.large_cluster_config(dag_config.cluster_name),
            databricks_conn_id=config.databricks_conn_id,
            ingested_bucket=config.ingested_bucket,
            intermediate_bucket=config.intermediate_bucket,
            spark_jobs_jar=config.spark_jobs_jar,
            wasb_conn_id=config.wasb_conn_id,
            wasb_raw_container=config.wasb_raw_container,
        )
        .has_ingest_from_ohub1(raw_bucket=config.raw_bucket)
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()

