from unittest import TestCase
from datetime import datetime
from ohub.operators.databricks_operator import BaseDatabricksOperator, DatabricksSubmitRunOperator,
                                               DatabricksCreateClusterOperator, DatabricksStartClusterOperator,
                                               DatabricksTerminateClusterOperator, DatabricksUninstallLibrariesOperator
# from ohub.utils.test import make_dag
from ohub.utils.databricks import find_cluster_id, get_cluster_status

###########################################################
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import State

def make_dag(date):
    now = datetime.utcnow()

    dag = DAG(
        dag_id='op_test', default_args={
            'owner': 'airflow',
            'retries': 3,
            'start_date': date
        },
        schedule_interval='@daily',
        dagrun_timeout=timedelta(minutes=60))

    dag.create_dagrun(
        run_id='manual__' + date.isoformat(),
        execution_date=date,
        start_date=now,
        state=State.RUNNING,
        external_trigger=False,
    )

    return dag
###########################################################

date = datetime(2016, 1, 1)


# class DatabricksCreateClusterOperatorTestCase(TestCase):
#     def test_DatabricksCreateClusterOperator(self):
#         DatabricksCreateClusterOperator(
#             task_id='DatabricksCreateClusterOperator',
#             dag=make_dag(date),
#             cluster_config=cluster_config,
#             databricks_conn_id="databricks_default",
#             polling_period_seconds=30,
#             databricks_retry_limit=3,
#         ).run(date, date, ignore_ti_state=True)


# class DatabricksStartClusterOperatorTestCase(TestCase):
#     def test_DatabricksStartClusterOperator(self):
#         DatabricksStartClusterOperator(
#             task_id='DatabricksStartClusterOperator',
#             dag=make_dag(date),
#             cluster_id=cluster_id,
#             databricks_conn_id="databricks_default",
#             polling_period_seconds=30,
#             databricks_retry_limit=3,
#         ).run(date, date, ignore_ti_state=True)


# class DatabricksSubmitRunOperatorTestCase(TestCase):
#     def test_DatabricksSubmitRunOperator(self):
#         DatabricksSubmitRunOperator(
#             task_id='DatabricksSubmitRunOperator',
#             dag=make_dag(date),
#             spark_jar_task=None,
#             spark_python_task=None,
#             notebook_task='1782139382823527',
#             libraries=None,
#             run_name='DatabricksSubmitRunOperator',
#             timeout_seconds=10,
#             cluster_name='DatabricksSubmitRunOperator',
#         ).run(date, date, ignore_ti_state=True)


# class DatabricksTerminateClusterOperatorTestCase(TestCase):
#     def test_DatabricksTerminateClusterOperator(self):
#         DatabricksTerminateClusterOperator(
#             task_id='DatabricksTerminateClusterOperator',
#             dag=make_dag(date),
#             cluster_name=None,
#             cluster_id=None,
#             cluster_config=None,
#             databricks_conn_id="databricks_default",
#             polling_period_seconds=30,
#             databricks_retry_limit=3,
#         ).run(date, date, ignore_ti_state=True)


# class DatabricksUninstallLibrariesOperatorTestCase(TestCase):
#     def test_DatabricksUninstallLibrariesOperator(self):
#         DatabricksUninstallLibrariesOperator(
#             cluster_id=cluster_id,
#             libraries_to_uninstall=libraries_to_uninstall,
#             databricks_conn_id="databricks_default",
#             polling_period_seconds=30,
#             databricks_retry_limit=3,
#         ).run(date, date, ignore_ti_state=True)

class DatabricksOperatorTestCase(TestCase):
    def test_DatabricksOperators(self):
        with make_dag(date) as dag:
            with "databricks_default" as databricks_conn_id:
                with 30 as polling_period_seconds:
                    with 3 as databricks_retry_limit:
                        cluster_name = 'test_cluster'
                        cluster_config={
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
                        ).run(date, date, ignore_ti_state=True)

                        with find_cluster_id(cluster_name=cluster_name, databricks_conn_id=databricks_conn_id) as cluster_id:

                            DatabricksStartClusterOperator(
                                task_id='DatabricksStartClusterOperator',
                            ).run(date, date, ignore_ti_state=True)

                            DatabricksSubmitRunOperator(
                                task_id='DatabricksSubmitRunOperator',
                                spark_jar_task=None,
                                spark_python_task=None,
                                notebook_task='1782139382823527',
                                libraries=[],
                                run_name='DatabricksSubmitRunOperator',
                                existing_cluster_id=cluster_id,
                            ).run(date, date, ignore_ti_state=True)

                            DatabricksUninstallLibrariesOperator(
                                task_id='DatabricksUninstallLibrariesOperator',
                                libraries_to_uninstall=[],
                            ).run(date, date, ignore_ti_state=True)

                            DatabricksTerminateClusterOperator(
                                task_id='DatabricksTerminateClusterOperator',
                            ).run(date, date, ignore_ti_state=True)
