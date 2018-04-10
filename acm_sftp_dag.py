from datetime import datetime

import os
from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.contrib.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 2),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

rfp = "/incoming/OHUB_2_testing/quoted_semi_colon_delimited/"
fds = "{{macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d')}}"
local_filepath = "/tmp/acm/{{ds}}/"
templated_local_orderlines_filepath = local_filepath + "ORDERLINES.csv"
templated_remote_orderlines_filepath = rfp + "UFS_ORDERLINES_" + fds + "*.csv"
templated_local_orders_filepath = local_filepath + "ORDERS.csv"
templated_remote_orders_filepath = rfp + "UFS_ORDERS_" + fds + "*.csv"
templated_local_products_filepath = local_filepath + "PRODUCTS.csv"
templated_remote_products_filepath = rfp + "UFS_PRODUCTS_" + fds + "*.csv"

task_defaults = {
    'ssh_conn_id': 'acm_sftp_ssh',
    'remote_host': 'unilever-z8i53y',
    'operation': SFTPOperation.GET
}

with DAG('acm_sftp_dag', default_args=default_args,
         schedule_interval="@once") as dag:

    mkdir = BashOperator('mkdir -p ' + local_filepath)
  
    fetch_order_lines = SFTPOperator(
        task_id='fetch_ACM_order_lines_for_date',
        local_filepath=templated_local_orderlines_filepath,
        remote_filepath=templated_remote_orderlines_filepath,
        **task_defaults)

    fetch_orders = SFTPOperator(
        task_id='fetch_ACM_orders_for_date',
        local_filepath=templated_local_orders_filepath,
        remote_filepath=templated_remote_orders_filepath,
        **task_defaults)

    fetch_products = SFTPOperator(
        task_id='fetch_ACM_products_for_date',
        local_filepath=templated_local_products_filepath,
        remote_filepath=templated_remote_products_filepath,
        **task_defaults)

    mkdir.set_downstream([fetch_order_lines, fetch_orders, fetch_products])
