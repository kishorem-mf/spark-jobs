from airflow import DAG

from ohub_dag_config import default_args, interval

from ohub2.operators_pipeline import construct_operators_pipeline
from ohub2.contact_persons_pipeline import construct_contact_persons_pipeline

with DAG('ohub2.0_full', default_args=default_args,
         schedule_interval=interval) as dag:
    operators_tasks = construct_operators_pipeline('foo')
    contact_persons_tasks = construct_contact_persons_pipeline('bar')

    operators_tasks['operators_ingest'] >> contact_persons_tasks['contact_persons_update_references']
