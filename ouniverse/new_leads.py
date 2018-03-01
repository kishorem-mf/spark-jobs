from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, \
    DataProcSparkOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

cluster_defaults = {
    'gcp_conn_id': 'airflow-sp',
    'cluster_name': 'ouniverse-new-leads',
    'project_id': 'ufs-prod',
    'region': 'global', # has to be global due to bug in airflow dataproc code
}

gs_jar_bucket = 'gs://ufs-prod/deployment/ouniverse'
gs_data_bucket = 'gs://ufs-prod/data/raw/ouniverse'

with DAG('new_leads', default_args=default_args,
         schedule_interval='@once') as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        num_workers=2,
        zone='europe-west4-c',
        **cluster_defaults)

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        **cluster_defaults)

    phase_one = DataProcSparkOperator(
        task_id="phase_one",
        main_class='com.unilever.ouniverse.leads.OperatorMapsSearcher',
        dataproc_spark_jars=[f'{gs_jar_bucket}/ouniverse-prioritisation-assembly-1.0.0-SNAPSHOT.jar'],
        cluster_name=cluster_defaults['cluster_name'],
        gcp_conn_id=cluster_defaults['gcp_conn_id'],
        arguments=[f'--operators {gs_data_bucket}/input/phase_I_NZ_sample.csv',
                   f'--outputpath {gs_data_bucket}/output/phaseI_output',
                   '--apiKey {{ google_api_key}}'])

    phase_two_grid = BashOperator(
        task_id="phase_two_grid",
        bash_command='echo "execute ouniverse phase II grid search"')

    phase_two_ids = BashOperator(
        task_id="phase_two_ids",
        bash_command='echo "execute ouniverse phase II id metadata"')

    prioritize = BashOperator(
        task_id="prioritise_leads",
        bash_command='echo "execute spark job"')

create_cluster >> phase_one >> prioritize
create_cluster >> phase_two_grid >> phase_two_ids >> prioritize
prioritize >> delete_cluster
