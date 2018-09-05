from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import State
import re
import pytest
from unittest import TestCase


def make_dag(dag_id):
    now = datetime.utcnow()
    suffix = re.sub(r'[^A-Za-z0-9_\-\.]+', '', str(now))
    dag = DAG(default_args={'start_date': now}, dag_id=dag_id + '_' + suffix)
    return dag


@pytest.fixture
def test_dag(dag):
    print(f'\n')
    dag._schedule_interval = timedelta(days=1)  # override cuz @once gets skipped
    done = set([])

    def run(key):
        print(f'running task {key}...')
        task = dag.task_dict[key]
        for k in task._upstream_task_ids:
            run(k)
        if key not in done:
            date = dag.default_args['start_date']
            task.run(date, date, ignore_ti_state=True)
            done.add(key)
    for k, _ in dag.task_dict.items():
        run(k)
