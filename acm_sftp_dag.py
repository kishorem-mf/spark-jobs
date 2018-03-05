from datetime import datetime

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation


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

templated_local_filepath = "/tmp/acm/{{ds}}/???"
templated_remote_filepath = "/incoming/OHUB_2_testing/quoted_semi_colon_delimited/???"

t1 = SFTPOperator(
    task_id='Fetch ACM files for date',
    dag=dag,
    ssh_hook=acm_ssh_hook,
    remote_host='unilever-z8i53y',
    local_filepath=templated_local_filepath,
    remote_filepath=templated_remote_filepath,
    operation=SFTPOperation.GET)
