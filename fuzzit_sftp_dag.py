from datetime import datetime

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from custom_operators.sftp_operator import SFTPOperator

from spark_job_config import ssh_hook, spark_cmd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

fuzzit_ssh_hook = SSHHook(ssh_conn_id='fuzzit_sftp_ssh')

dag = DAG('fuzzit_sftp_dag', default_args=default_args,
          schedule_interval="0 0 * * *")

t1 = SFTPOperator(
    task_id='Fetch all Fuzzit files',
    dag=dag,
    ssh_hook=fuzzit_ssh_hook,
    remote_host='apps.systrion.eu',
    destination_folder={'fuzzit_destination_folder': 'The folder to store files from the Fuzzit SFTP server in'})
