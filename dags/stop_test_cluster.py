from airflow import DAG
from datetime import datetime, timedelta

from dags import config
from ohub.operators.databricks_operator import (
    DatabricksTerminateClusterOperator
)

""""Stop the test cluster and the end of each working day"""
dag_args = {
    **config.dag_default_args,
    **{
        "start_date": datetime(2018, 8, 8),
        "retry_delay": timedelta(minutes=2),
        "wait_for_downstream": False,
        "depends_on_past": False,
    },
}

with DAG("stop_test_cluster", default_args=dag_args, schedule_interval="0 18 * * *") as dag:
    cluster_conf = config.cluster_config(test=True)
    stop_cluster = DatabricksTerminateClusterOperator(
        task_id="stop_cluster",
        cluster_config=cluster_conf,
        cluster_name=cluster_conf['cluster_name'],
        databricks_conn_id=cluster_conf.get('databricks_conn_id'),
    )
    stop_cluster
