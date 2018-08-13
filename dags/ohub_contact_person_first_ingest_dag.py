from datetime import datetime

from airflow import DAG

from dags import config
from dags.config import start_date_first
from ohub.operators.databricks_operator import DatabricksSubmitRunOperator
from ohub.operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub.utils.airflow import DagConfig, GenericPipeline, SubPipeline

dag_args = {
    **config.dag_default_args,
    **{
        "start_date": start_date_first,
        "pool": "ohub_contactpersons_pool",
    },
}

entity = "contactpersons"
dag_config = DagConfig(entity, is_delta=False)

with DAG(
    dag_config.dag_id, default_args=dag_args, schedule_interval=dag_config.schedule
) as dag:
    generic = (
        GenericPipeline(
            dag_config,
            class_prefix="ContactPerson",
            cluster_config=config.large_cluster_config(dag_config.cluster_name),
            databricks_conn_id=config.databricks_conn_id,
            ingested_bucket=config.ingested_bucket,
            intermediate_bucket=config.intermediate_bucket,
            spark_jobs_jar=config.spark_jobs_jar,
            wasb_conn_id=config.wasb_conn_id,
            wasb_raw_container=config.wasb_raw_container,
        )
        .has_export_to_acm(
            acm_schema_name="RECIPIENTS",
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
            container_name=config.container_name,
            wasb_export_container=config.wasb_export_container,
        )
        .has_export_to_dispatcher_db(
            dispatcher_schema_name="CONTACT_PERSONS",
            integrated_bucket=config.integrated_bucket,
            export_bucket=config.export_bucket,
        )
        .has_ingest_from_ohub1(raw_bucket=config.raw_bucket)
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        match_py="dbfs:/libraries/name_matching/match_contacts.py",
        ingest_input=config.intermediate_bucket.format(
            date="{{ds}}", fn="{}_left_overs".format(entity)
        ),
        ohub_country_codes=config.ohub_country_codes,
        string_matching_egg=config.string_matching_egg,
    )

    exact_match = DatabricksSubmitRunOperator(
        task_id="{}_exact_match".format(entity),
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.ContactPersonExactMatcher",
            "parameters": [
                "--inputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn=f"{entity}_gathered"
                ),
                "--exactMatchOutputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_exact_matches".format(entity)
                ),
                "--leftOversOutputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_left_overs".format(entity)
                )
            ],
        },
    )

    join = DatabricksSubmitRunOperator(
        task_id="{}_merge".format(entity),
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            "parameters": [
                "--matchingInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_matched".format(entity)
                ),
                "--entityInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_left_overs".format(entity)
                ),
                "--outputFile",
                config.intermediate_bucket.format(date="{{ ds }}", fn=entity),
            ],
        },
    )

    combine = DatabricksSubmitRunOperator(
        task_id="{}_combining".format(entity),
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.combining.ContactPersonCombining",
            "parameters": [
                "--integratedUpdated",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_exact_matches".format(entity)
                ),
                "--newGolden",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn=entity, channel="*"
                ),
                "--combinedEntities",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_combined".format(entity)
                ),
            ],
        },
    )

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id="operators_integrated_sensor",
        external_dag_id="ohub_operators_initial_load",
        external_task_id="join",
    )

    referencing = DatabricksSubmitRunOperator(
        task_id="{}_referencing".format(entity),
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.ContactPersonReferencing",
            "parameters": [
                "--combinedInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_combined".format(entity)
                ),
                "--operatorInputFile",
                config.integrated_bucket.format(
                    date="{{ ds }}", fn="operators", channel="*"
                ),
                "--outputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_updated_references".format(entity)
                ),
            ],
        },
    )

    update_golden_records = DatabricksSubmitRunOperator(
        task_id="update_golden_records",
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.ContactPersonUpdateGoldenRecord",
            "parameters": [
                "--inputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_updated_references".format(entity)
                ),
                "--outputFile",
                config.integrated_bucket.format(date="{{ ds }}", fn=entity),
            ],
        },
    )

    ingest.last_task >> exact_match >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> join >> combine >> operators_integrated_sensor >> referencing >> update_golden_records >> export.first_task
