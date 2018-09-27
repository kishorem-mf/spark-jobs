from airflow import DAG
from datetime import datetime, timedelta

from dags import config
from ohub.operators.databricks_operator import (
    DatabricksCreateClusterOperator,
    DatabricksTerminateClusterOperator,
    DatabricksSubmitRunOperator,
)
from ohub.operators.ga_fetch_operator import GAToGSOperator, GSToLocalOperator, LocalGAToWasbOperator

dag_args = {
    **config.dag_default_args,
    **{
        "start_date": datetime(2018, 7, 27),
        "retry_delay": timedelta(minutes=2),
        "wait_for_downstream": False,
        "depends_on_past": False,
    },
}

local_path = "/tmp/gs_export/"
remote_bucket = "digitaldataufs"
path_in_bucket = "ga_data"
cluster_id = "0405-082501-flare296"

with DAG("gcp_ga", default_args=dag_args, schedule_interval="0 4 * * *") as dag:
    ga_to_gs = GAToGSOperator(
        task_id="fetch_GA_from_BQ_for_date",
        bigquery_conn_id="gcp_storage",
        destination=f"gs://{remote_bucket}/{path_in_bucket}",
        date="{{ yesterday_ds }}",
        country_codes=config.country_codes,
    )

    gs_to_local = GSToLocalOperator(
        task_id="gcp_bucket_to_local",
        path=local_path,
        date="{{ yesterday_ds }}",
        bucket=remote_bucket,
        path_in_bucket=path_in_bucket,
        gcp_conn_id="gcp_storage",
        country_codes=config.country_codes,
    )

    local_to_wasb = LocalGAToWasbOperator(
        task_id="local_to_azure",
        wasb_conn_id="azure_blob",
        path=local_path,
        date="{{ yesterday_ds }}",
        country_codes=config.country_codes,
        container_name="prod",
        blob_path="data/raw/gaData/",
    )

    start_cluster = DatabricksCreateClusterOperator(
        cluster_config=config.cluster_config('reco'),
        task_id="start_cluster",
        cluster_id=cluster_id,
        databricks_conn_id=config.databricks_conn_id,
    )

    terminate_cluster = DatabricksTerminateClusterOperator(
        task_id="terminate_cluster",
        cluster_id=cluster_id,
        databricks_conn_id=config.databricks_conn_id,
    )

    update_ga_table = DatabricksSubmitRunOperator(
        task_id="update_ga_table",
        existing_cluster_id="0405-082501-flare296",
        databricks_conn_id=config.databricks_conn_id,
        notebook_task={
            "notebook_path": "/Users/tim.vancann@unilever.com/update_ga_tables"  # TODO: get rid of user tim in the path
        },
    )

    ga_to_gs >> gs_to_local >> local_to_wasb >> update_ga_table
    start_cluster >> update_ga_table >> terminate_cluster
