from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator

from spark_job_config import spark_cmd, ssh_hook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('new_leads', default_args=default_args,
          schedule_interval='0 0 1 * *') as dag:

    phase_one = BashOperator(
        task_id="phase_one",
        bash_command='echo "execute ouniverse phase I"')

    phase_two_grid = BashOperator(
        task_id="phase_two_grid",
        bash_command='echo "execute ouniverse phase II grid search"')

    phase_two_ids = BashOperator(
        task_id="phase_two_ids",
        bash_command='echo "execute ouniverse phase II id metadata"')

    prioritize = SSHOperator(
        task_id="prioritise_leads",
        bash_command=spark_cmd(
            jar='/ouniverse/universe-ingestion-spark-assembly-1.0.0-SNAPSHOT.jar',
            main_class='com.unilever.ouniverse.leads.PrioritizeLeads',
            args="--operators '*_output.avro' --places '*_details.avro' \
            --leads 'cities.csv'--priorities 'priorities.csv' \
            --outputpath 'here'"),
        ssh_hook=ssh_hook)

phase_one >> prioritize
phase_two_grid >> phase_two_ids >> prioritize
