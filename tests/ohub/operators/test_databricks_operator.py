from unittest import TestCase
from datetime import datetime
from ohub.operators.databricks_operator import BaseDatabricksOperator, DatabricksSubmitRunOperator, \
                                               DatabricksCreateClusterOperator, DatabricksStartClusterOperator, \
                                               DatabricksTerminateClusterOperator, DatabricksUninstallLibrariesOperator
from testing.utils.test import make_dag
from ohub.utils.databricks import find_cluster_id, get_cluster_status

date = datetime(2016, 1, 1)


class DatabricksOperatorTestCase(TestCase):
    def test_DatabricksOperators(self):
        with make_dag('DatabricksOperatorTestCase') as dag:
            databricks_conn_id = "databricks_default"
            polling_period_seconds = 30
            databricks_retry_limit = 3
            cluster_name = 'test_cluster'
            cluster_config = {
                "cluster_name": cluster_name,
                "reuse_cluster": '0',
                "spark_version": "4.0.x-scala2.11",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": "1",
                "autotermination_minutes": "1",
                "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            }

            DatabricksCreateClusterOperator(
                task_id='DatabricksCreateClusterOperator',
                cluster_config=cluster_config,
                databricks_conn_id=databricks_conn_id,
                polling_period_seconds=polling_period_seconds,
                databricks_retry_limit=databricks_retry_limit,
            ).run(date, date, ignore_ti_state=True)

            with find_cluster_id(cluster_name=cluster_name, databricks_conn_id=databricks_conn_id) as cluster_id:

                DatabricksStartClusterOperator(
                    task_id='DatabricksStartClusterOperator',
                    databricks_conn_id=databricks_conn_id,
                    polling_period_seconds=polling_period_seconds,
                    databricks_retry_limit=databricks_retry_limit,
                ).run(date, date, ignore_ti_state=True)

                DatabricksSubmitRunOperator(
                    task_id='DatabricksSubmitRunOperator',
                    spark_jar_task=None,
                    spark_python_task=None,
                    notebook_task='1782139382823527',
                    libraries=[],
                    run_name='DatabricksSubmitRunOperator',
                    existing_cluster_id=cluster_id,
                    databricks_conn_id=databricks_conn_id,
                    polling_period_seconds=polling_period_seconds,
                    databricks_retry_limit=databricks_retry_limit,
                ).run(date, date, ignore_ti_state=True)

                DatabricksUninstallLibrariesOperator(
                    task_id='DatabricksUninstallLibrariesOperator',
                    libraries_to_uninstall=[],
                    databricks_conn_id=databricks_conn_id,
                    polling_period_seconds=polling_period_seconds,
                    databricks_retry_limit=databricks_retry_limit,
                ).run(date, date, ignore_ti_state=True)

                DatabricksTerminateClusterOperator(
                    task_id='DatabricksTerminateClusterOperator',
                    databricks_conn_id=databricks_conn_id,
                    polling_period_seconds=polling_period_seconds,
                    databricks_retry_limit=databricks_retry_limit,
                ).run(date, date, ignore_ti_state=True)
