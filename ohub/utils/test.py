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
