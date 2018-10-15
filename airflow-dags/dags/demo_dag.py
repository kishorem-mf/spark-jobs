"""DAG for testing functionality in the UI"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dags.config import dag_default_args

DAG_ARGS = {
    **dag_default_args,
    **{"start_date": datetime(2018, 10, 1), "retries": 0, "wait_for_downstream": False},
}

with DAG(dag_id="demo_dag", default_args=DAG_ARGS, schedule_interval="@daily") as dag:

    def _print_exec_date(**context):
        print(context["execution_date"])

    print_exec_date = PythonOperator(
        task_id="print_exec_date",
        python_callable=_print_exec_date,
        provide_context=True,
    )
