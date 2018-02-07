from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.operators.bash_operator import BashOperator

from spark_job_config import spark_cmd, ssh_hook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('new_leads', default_args=default_args)

phase_one = BashOperator(
    task_id="execute_bash_command",
    bash_command='echo "execute ouniverse phase I"',
    dag=dag)

phase_two_grid = BashOperator(
    task_id="execute_bash_command",
    bash_command='echo "execute ouniverse phase II grid search"',
    dag=dag)

phase_two_ids = BashOperator(
    task_id="execute_bash_command",
    bash_command='echo "execute ouniverse phase II id metadata"',
    dag=dag)

prioritize = SSHExecuteOperator(
    task_id="execute_bash_command",
    bash_command=spark_cmd(jar='/ouniverse/universe-ingestion-spark-assembly-1.0.0-SNAPSHOT.jar',
                           main_class='com.unilever.ouniverse.leads.PrioritizeLeads',
                           args="--operators '*_output.avro' --places '*_details.avro' --leads 'cities.csv'\
                            --priorities 'priorities.csv' --outputpath 'here'"),
    ssh_hook=ssh_hook,
    dag=dag)


phase_one.set_downstream(prioritize)
phase_two_grid.set_downstream(phase_two_ids)
phase_two_ids.set_downstream(prioritize)
