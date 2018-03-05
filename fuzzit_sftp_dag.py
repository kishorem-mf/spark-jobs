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

fuzzit_ssh_hook = SSHHook(ssh_conn_id='fuzzit_sftp_ssh')
templated_local_filepath = "/tmp/fuzzit/{{ds}}/UFS_Fuzzit_OHUB20_1400.zip"
templated_remote_filepath = "./UFS_Fuzzit_OHUB20_{{macros.ds_format(ds, '%Y%m%d')}}_1400.zip"

with DAG('fuzzit_sftp_dag', default_args=default_args,
          schedule_interval="0 0 * * *") as dag:
    t1 = SFTPOperator(
        task_id='Fetch Fuzzit files for date',
        dag=dag,
        ssh_hook=fuzzit_ssh_hook,
        remote_host='apps.systrion.eu',
        local_filepath=templated_local_filepath,
        remote_filepath=templated_remote_filepath,
        operation=SFTPOperation.GET)
