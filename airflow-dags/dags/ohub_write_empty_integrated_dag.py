
from airflow import DAG

from dags import config
from dags.config import start_date_delta, start_date_first

from ohub.operators.databricks_operator import (
    DatabricksCreateClusterOperator,
    DatabricksTerminateClusterOperator,
    DatabricksSubmitRunOperator,
)

from airflow.utils.trigger_rule import TriggerRule

dag_args = {
    **config.dag_default_args,
    **{
        "start_date": start_date_first
    },
}

with DAG(
    "empty_integrated", default_args=dag_args, schedule_interval="@once"
) as dag:
    cluster_conf = config.cluster_config("empty_integrated_initial", large=False)

    start_cluster = DatabricksCreateClusterOperator(
        task_id="start_cluster",
        databricks_conn_id=config.databricks_conn_id,
        cluster_config=cluster_conf,
    )

    terminate_cluster = DatabricksTerminateClusterOperator(
        task_id="terminate_cluster",
        cluster_name=cluster_conf['cluster_name'],
        cluster_config=cluster_conf,
        databricks_conn_id=config.databricks_conn_id,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    for key, value in config.ohub_entities.items():
        clazz = value["spark_class"]

        empty_integrated = DatabricksSubmitRunOperator(
            task_id="empty_integrated_{}".format(key),
            cluster_name=cluster_conf['cluster_name'],
            databricks_conn_id=config.databricks_conn_id,
            libraries=[{"jar": config.spark_jobs_jar}],
            spark_jar_task={
                "main_class_name": "com.unilever.ohub.spark.ingest.initial.{}EmptyIntegratedWriter".format(clazz),
                "parameters": [
                    "--outputFile",
                    config.integrated_bucket.format(date="{{ ds }}", fn=key),
                ],
            },
        )

        start_cluster >> empty_integrated >> terminate_cluster
