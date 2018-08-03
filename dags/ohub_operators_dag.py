from datetime import datetime

from airflow import DAG

from dags import config
from ohub.operators.databricks_operator import DatabricksSubmitRunOperator
from ohub.utils.airflow import DagConfig, GenericPipeline, SubPipeline, LazyConnection

dag_args = {
    **config.dag_default_args,
    **{
        "start_date": datetime(2018, 7, 26),
        "pool": "ohub_operators_pool",
    },
}

entity = "operators"
dag_config = DagConfig(entity, is_delta=True)

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
        .has_ingest_from_file_interface(raw_bucket=config.raw_bucket)
        .has_ingest_from_web_event(
            raw_bucket=config.raw_bucket, ingested_bucket=config.ingested_bucket
        )
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()
    fuzzy_matching: SubPipeline = generic.construct_fuzzy_matching_pipeline(
        ingest_input=config.intermediate_bucket.format(
            date="{{ ds }}", fn=f"{entity}_gathered"
        ),
        match_py="dbfs:/libraries/name_matching/match_operators.py",
        integrated_input=config.integrated_bucket.format(
            date="{{ yesterday_ds }}", fn=dag_config.entity
        ),
        delta_match_py="dbfs:/libraries/name_matching/delta_operators.py",
        ohub_country_codes=config.ohub_country_codes,
        string_matching_egg=config.string_matching_egg,
    )

    join = DatabricksSubmitRunOperator(
        task_id="merge",
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
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

    combine_to_create_integrated = DatabricksSubmitRunOperator(
        task_id="combine_to_create_integrated",
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        libraries=[{"jar": config.spark_jobs_jar}],
        spark_jar_task={
            "main_class_name": "com.unilever.ohub.spark.combining.OperatorCombining",
            "parameters": [
                "--integratedUpdated",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_fuzzy_matched_delta_integrated".format(entity)
                ),
                "--newGolden",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_delta_golden_records".format(entity)
                ),
                "--combinedEntities",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_combined".format(entity)
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
            "main_class_name": "com.unilever.ohub.spark.merging.OperatorUpdateGoldenRecord",
            "parameters": [
                "--inputFile",
                config.intermediate_bucket.format(
                    date="{{ ds }}", fn="{}_combined".format(entity)
                ),
                "--outputFile",
                config.integrated_bucket.format(date="{{ ds }}", fn=entity),
            ],
        },
    )

    update_operators_table = DatabricksSubmitRunOperator(
        task_id="update_table",
        cluster_name=dag_config.cluster_name,
        databricks_conn_id=config.databricks_conn_id,
        notebook_task={
            "notebook_path": "/Users/tim.vancann@unilever.com/update_integrated_tables"  # TODO: remove code from notebooks asap
        },
    )

    ingest.last_task >> fuzzy_matching.first_task
    fuzzy_matching.last_task >> join >> combine_to_create_integrated >> update_golden_records
    update_golden_records >> update_operators_table
    update_golden_records >> export.first_task
    ingest.first_task >> export.last_task
