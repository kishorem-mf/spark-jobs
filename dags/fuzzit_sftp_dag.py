from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperation
from airflow.operators.bash_operator import BashOperator

from ohub.operators.zip_operator import UnzipOperator
from ohub.operators.wasb_operator import FolderToWasbOperator
from ohub.operators.short_circuit_sftp_operator import ShortCircuitSFTPOperator
from dags.config import container_name, dag_default_args

dag_args = {**dag_default_args, **{"start_date": datetime(2018, 3, 2), "retries": 0}}

with DAG(
    dag_id="fuzzit_sftp", default_args=dag_args, schedule_interval="0 0 1 * *"
) as dag:
    fds = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"
    remote_filepath = f"/ftp/ftp_ohub20/UFS_Fuzzit_OHUB20_{fds}_1400.zip"
    local_filepath = "/tmp/fuzzit/{{ ds }}/UFS_Fuzzit_OHUB20_1400.zip"
    path_to_unzip_contents = "/tmp/fuzzit/{{ ds }}/csv/"

    mkdir = BashOperator(
        bash_command=f"mkdir -p {path_to_unzip_contents}", task_id="mkdir_fuzzit"
    )

    fetch = ShortCircuitSFTPOperator(
        task_id="fetch_fuzzit_files_for_date",
        ssh_conn_id="fuzzit_sftp_ssh",
        remote_host="apps.systrion.eu",
        local_filepath=local_filepath,
        remote_filepath=remote_filepath,
        operation=SFTPOperation.GET,
    )

    unzip = UnzipOperator(
        task_id="unzip_fuzzit_file",
        path_to_zip_file=local_filepath,
        path_to_unzip_contents=path_to_unzip_contents,
        dag=dag,
    )

    wasb = FolderToWasbOperator(
        task_id="fuzzit_to_wasb",
        folder_path=path_to_unzip_contents,
        blob_name="data/raw/fuzzit/{{ds}}",
        container_name=container_name,
    )

    mkdir >> fetch >> unzip >> wasb
