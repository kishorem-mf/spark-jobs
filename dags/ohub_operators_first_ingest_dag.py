from datetime import datetime

from airflow import DAG

from dags import config
from dags.config import start_date_first
from ohub.operators.databricks_operator import DatabricksSubmitRunOperator
from ohub.utils.airflow import DagConfig, GenericPipeline, SubPipeline

dag_args = {
    **config.dag_default_args,
    **{
        "start_date": start_date_first,
        "pool": "ohub_operators_pool",
    },
}

entity = "operators"
dag_config = DagConfig(entity, is_delta=False)

with DAG(
    dag_config.dag_id, default_args=dag_args, schedule_interval=dag_config.schedule
) as dag:
    generic = (
        GenericPipeline(
            dag_config,
            class_prefix="Operator",
            cluster_config=config.large_cluster_config(dag_config.cluster_name),
            databricks_conn_id=config.databricks_conn_id,
            ingested_bucket=config.ingested_bucket,
            intermediate_bucket=config.intermediate_bucket,
            spark_jobs_jar=config.spark_jobs_jar,
            wasb_conn_id=config.wasb_conn_id,
            wasb_raw_container=config.wasb_raw_container,
        )
        .has_ingest_from_file_interface(raw_bucket=config.raw_bucket)
        .has_export_to_acm(
            acm_schema_name="OPERATORS",
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
            container_name=config.container_name,
            wasb_export_container=config.wasb_export_container,
        )
        .has_export_to_dispatcher_db(
            dispatcher_schema_name="OPERATORS",
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
        )
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        match_py="dbfs:/libraries/name_matching/match_operators.py",
        ingest_input=config.intermediate_bucket.format(
            date="{{ ds }}", fn=f"{entity}_gathered"
        ),
        ohub_country_codes=config.ohub_country_codes,
        string_matching_egg=config.string_matching_egg,
    )

    join = DatabricksSubmitRunOperator(
        task_id="join",
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            "parameters": [
                "--matchingInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_matched".format(entity)
                ),
                "--entityInputFile",
                config.ingested_bucket.format(date="{{ ds }}", fn=entity, channel="*"),
                "--outputFile",
                config.integrated_bucket.format(date="{{ ds }}", fn=entity),
            ],
        },
    )

    ingest.last_task >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> join >> export.first_task
