from datetime import datetime

import os
from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.operators.bash_operator import BashOperator
from custom_operators.folder_to_wasb import FolderToWasbOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

rfp = "/incoming/OHUB_2_testing/quoted_semi_colon_delimited/"
fds = "{{macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d')}}"
local_filepath = "/tmp/acm/{{ds}}/"

task_defaults = {
    'ssh_conn_id': 'acm_sftp_ssh',
    'operation': SFTPOperation.GET
}

with DAG('acm_sftp_dag', default_args=default_args,
         schedule_interval="@once") as dag:

    mkdir = BashOperator(
        bash_command='mkdir -p ' + local_filepath,
        task_id='mkdir_acm',
    )

    fetch_order_lines = SFTPOperator(
        task_id='fetch_ACM_order_lines_for_date',
        local_filepath=local_filepath + "ORDERLINES.csv",
        remote_filepath=rfp + "UFS_ORDERLINES_" + fds + "*.csv",
        **task_defaults)

    fetch_orders = SFTPOperator(
        task_id='fetch_ACM_orders_for_date',
        local_filepath=local_filepath + "ORDERS.csv",
        remote_filepath=rfp + "UFS_ORDERS_" + fds + "*.csv",
        **task_defaults)

    fetch_products = SFTPOperator(
        task_id='fetch_ACM_products_for_date',
        local_filepath=local_filepath + "PRODUCTS.csv",
        remote_filepath=rfp + "UFS_PRODUCTS_" + fds + "*.csv",
        **task_defaults)

    wasb = FolderToWasbOperator(
        task_id='acm_to_wasb',
        folder_path=local_filepath,
        container_name='prod/data/raw/acm/{{ds}}',
    )

    fetches = [fetch_order_lines, fetch_orders, fetch_products]
    mkdir.set_downstream(fetches)
    wasb.set_upstream(fetches)
