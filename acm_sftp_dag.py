from datetime import datetime

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from custom_operators.sftp_operator import SFTPOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

acm_ssh_hook = SSHHook(ssh_conn_id='acm_sftp_ssh')

dag = DAG('acm_sftp_dag', default_args=default_args,
          schedule_interval="0 0 * * *")

t1 = SFTPOperator(
    task_id='Fetch all ACM files',
    dag=dag,
    ssh_hook=acm_ssh_hook,
    remote_host={'acm_remote_host'},
    remote_folder={'acm_remote_folder'},
    destination_folder={'acm_remote_host'})
