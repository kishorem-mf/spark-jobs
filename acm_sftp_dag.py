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

acm_ssh_hook = SSHHook(ssh_conn_id='acm_sftp_ssh')

dag = DAG('acm_sftp_dag', default_args=default_args,
          schedule_interval="0 0 * * *")

t1 = SFTPOperator(
    task_id='Fetch all ACM files',
    dag=dag,
    ssh_hook=acm_ssh_hook,
    remote_host='unilever-sftp.neolane.net',
    remote_folder='/incoming/OHUB_2_testing/quoted_semi_colon_delimited',
    destination_folder={'acm_destination_folder': 'The folder to store files from the ACM SFTP server in'})
