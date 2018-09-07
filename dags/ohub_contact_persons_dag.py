from datetime import datetime

from airflow import DAG

from dags import config
from dags.config import start_date_delta, start_date_first
from ohub.operators.databricks_operator import DatabricksSubmitRunOperator
from ohub.operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub.utils.airflow import DagConfig, GenericPipeline, SubPipeline

dag_args = {
    **config.dag_default_args,
    **{
        "start_date": start_date_delta,
        "pool": "ohub_contactpersons_pool"
    },
}

entity = "contactpersons"
dag_config = DagConfig(entity, is_delta=True)

with DAG(
    dag_config.dag_id, default_args=dag_args, schedule_interval=dag_config.schedule
) as dag:
    cluster_conf = config.cluster_config(dag_config.cluster_name, large=True)

    generic = (
        GenericPipeline(
            dag_config,
            class_prefix="ContactPerson",
            cluster_config=cluster_conf,
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
        .has_ingest_from_file_interface(raw_bucket=config.raw_bucket)
        .has_ingest_from_web_event(
            raw_bucket=config.raw_bucket, ingested_bucket=config.ingested_bucket
        )
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline(start_date_first)
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        delta_match_py="dbfs:/libraries/name_matching/delta_contacts.py",
        match_py="dbfs:/libraries/name_matching/match_contacts.py",
        integrated_input=config.intermediate_bucket.format(
            date="{{ ds }}", fn="{}_unmatched_integrated".format(entity)
        ),
        ingest_input=config.intermediate_bucket.format(
            date="{{ ds }}", fn="{}_unmatched_delta".format(entity)
        ),
        ohub_country_codes=config.ohub_country_codes,
        string_matching_egg=config.string_matching_egg,
    )

    pre_processing = DatabricksSubmitRunOperator(
        task_id="pre_processed",
        cluster_name=cluster_conf['cluster_name'],
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.ContactPersonPreProcess",
            "parameters": [
                "--integratedInputFile",
                config.integrated_bucket.format(date="{{ yesterday_ds }}", fn=entity),
                "--deltaInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn=f"{entity}_gathered"
                ),
                "--deltaPreProcessedOutputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_pre_processed".format(entity)
                ),
            ],
        },
    )

    exact_match_integrated_ingested = DatabricksSubmitRunOperator(
        task_id="exact_match_integrated_ingested",
        cluster_name=cluster_conf['cluster_name'],
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.ContactPersonIntegratedExactMatch",
            "parameters": [
                "--integratedInputFile",
                config.integrated_bucket.format(date="{{ yesterday_ds }}", fn=entity),
                "--deltaInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_pre_processed".format(entity)
                ),
                "--matchedExactOutputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_exact_matches".format(entity)
                ),
                "--unmatchedIntegratedOutputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_unmatched_integrated".format(entity)
                ),
                "--unmatchedDeltaOutputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_unmatched_delta".format(entity)
                ),
            ],
        },
    )

    join_fuzzy_matched = DatabricksSubmitRunOperator(
        task_id="join_matched",
        cluster_name=cluster_conf['cluster_name'],
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            "parameters": [
                "--matchingInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_fuzzy_matched_delta".format(entity)
                ),
                "--entityInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_delta_left_overs".format(entity)
                ),
                "--outputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_delta_golden_records".format(entity)
                ),
            ],
        },
    )

    join_fuzzy_and_exact_matched = DatabricksSubmitRunOperator(
        task_id="join_fuzzy_and_exact_matched",
        cluster_name=cluster_conf['cluster_name'],
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.combining.ContactPersonCombineExactAndFuzzyMatches",
            "parameters": [
                "--contactPersonExactMatchedInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_exact_matches".format(entity)
                ),
                "--contactPersonFuzzyMatchedDeltaIntegratedInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_fuzzy_matched_delta_integrated".format(entity)
                ),
                "--contactPersonsDeltaGoldenRecordsInputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_delta_golden_records".format(entity)
                ),
                "--contactPersonsCombinedOutputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_combined".format(entity)
                ),
            ],
        },
    )

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id="operators_integrated_sensor",
        external_dag_id="ohub_operators",
        external_task_id="update_golden_records",
    )

    referencing = DatabricksSubmitRunOperator(
        task_id="referencing",
        cluster_name=cluster_conf['cluster_name'],
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
                config.integrated_bucket.format(date="{{ ds }}", fn="operators"),
                "--outputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_updated_references".format(entity)
                ),
            ],
        },
    )

    update_golden_records = DatabricksSubmitRunOperator(
        task_id="update_golden_records",
        cluster_name=cluster_conf['cluster_name'],
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

    ingest.last_task >> pre_processing >> exact_match_integrated_ingested
    exact_match_integrated_ingested >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> join_fuzzy_matched >> join_fuzzy_and_exact_matched >> operators_integrated_sensor
    operators_integrated_sensor >> referencing >> update_golden_records >> export.first_task
    ingest.first_task >> export.last_task
