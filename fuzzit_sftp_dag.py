from datetime import datetime

import os
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from custom_operators.zip_operator import UnzipOperator
from custom_operators.folder_to_wasb import FolderToWasbOperator
from config import country_codes

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

fds = "{{macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d')}}"
fuzzit_ssh_hook = SSHHook(conn_id='fuzzit_sftp_ssh')
templated_remote_filepath = "./UFS_Fuzzit_OHUB20_" + fds + "_1400.zip"
templated_local_filepath = "/tmp/fuzzit/{{ds}}/UFS_Fuzzit_OHUB20_1400.zip"
templated_path_to_unzip_contents = '/tmp/fuzzit/{{ds}}/csv/'
# os.makedirs(templated_path_to_unzip_contents, True)
wasb_root_bucket = 'wasbs://prod@ulohub2storedevne.blob.core.windows.net/data/'

with DAG('fuzzit_sftp_dag', default_args=default_args,
         schedule_interval="@once") as dag:
    fetch = SFTPOperator(
        task_id='fetch_fuzzit_files_for_date',
        ssh_hook=fuzzit_ssh_hook,
        remote_host='apps.systrion.eu',
        local_filepath=templated_local_filepath,
        remote_filepath=templated_remote_filepath,
        operation=SFTPOperation.GET)

    unzip = UnzipOperator(
        task_id='unzip_fuzzit_file',
        path_to_zip_file=templated_local_filepath,
        path_to_unzip_contents=templated_path_to_unzip_contents,
        dag=dag)

    wasb = FolderToWasbOperator(
        task_id='fuzzit_to_wasb',
        folder_path=templated_path_to_unzip_contents,
        container_name='prod',
        blob_name='ulohub2storedevne',
    )

    fetch >> unzip >> wasb
