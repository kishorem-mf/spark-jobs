from datetime import datetime

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from custom_operators.zip_operator import UnzipOperator
from config import country_codes, shared_default

default_args = {
    **shared_default,
}

fds = "{{macros.ds_format(ds, '%Y%m%d')}}"
fuzzit_ssh_hook = SSHHook(ssh_conn_id='fuzzit_sftp_ssh')
templated_local_filepath = "/tmp/fuzzit/{{ds}}/UFS_Fuzzit_OHUB20_1400.zip"
templated_remote_filepath = "./UFS_Fuzzit_OHUB20_" + fds + "_1400.zip"
templated_path_to_unzip_contents = "./"

with DAG('fuzzit_sftp_dag', default_args=default_args,
         schedule_interval="0 0 * * *") as dag:
    fetch = SFTPOperator(
        task_id='fetch_fuzzit_files_for_date',
        ssh_hook=fuzzit_ssh_hook,
        remote_host='apps.systrion.eu',
        local_filepath=templated_local_filepath,
        remote_filepath=templated_remote_filepath,
        operation=SFTPOperation.GET)

    unzip = UnzipOperator(
        task_id='unzip_fuzzit_file',
        path_to_save_zip=templated_local_filepath,
        path_to_unzip_contents=templated_path_to_unzip_contents,
        dag=dag)

    fetch >> unzip
