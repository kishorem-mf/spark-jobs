from unittest import TestCase
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
# from ohub.utils.test import make_dag

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

class BashOperatorTestCase(TestCase):
    def test_bash(self):
        import tempfile
        with tempfile.NamedTemporaryFile() as f:
            BashOperator(
                task_id='bash',
                dag=make_dag(date),
                bash_command=f'echo 1 > {f.name};'
            ).run(date, date, ignore_ti_state=True)
            with open(f.name, 'r') as fr:
                self.assertIn('1', ''.join(fr.readlines()))
