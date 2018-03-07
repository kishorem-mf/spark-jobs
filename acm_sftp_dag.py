from datetime import datetime

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation


acm_ssh_hook = SSHHook(ssh_conn_id='acm_sftp_ssh')

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
fds = "{{macros.ds_format(ds, '%Y%m%d')}}"
local_filepath = "/tmp/acm/{{ds}}/"
templated_local_orderlines_filepath = local_filepath + "ORDERLINES.csv"
templated_remote_orderlines_filepath = rfp + "UFS_ORDERLINES_" + fds + "*.csv"
templated_local_orders_filepath = local_filepath + "ORDERS.csv"
templated_remote_orders_filepath = rfp + "UFS_ORDERS_" + fds + "*.csv"
templated_local_products_filepath = local_filepath + "PRODUCTS.csv"
templated_remote_products_filepath = rfp + "UFS_PRODUCTS_" + fds + "*.csv"

task_defaults = {
    'ssh_hook': acm_ssh_hook,
    'remote_host': 'unilever-z8i53y',
    'operation': SFTPOperation.GET
}

with DAG('acm_sftp_dag', default_args=default_args,
         schedule_interval="0 0 * * *") as dag:
    t1 = SFTPOperator(
        task_id='Fetch ACM order lines for date',
        local_filepath=templated_local_orderlines_filepath,
        remote_filepath=templated_remote_orderlines_filepath,
        **task_defaults)

    t2 = SFTPOperator(
        task_id='Fetch ACM orders for date',
        local_filepath=templated_local_orders_filepath,
        remote_filepath=templated_remote_orders_filepath,
        **task_defaults)

    t3 = SFTPOperator(
        task_id='Fetch ACM products for date',
        local_filepath=templated_local_products_filepath,
        remote_filepath=templated_remote_products_filepath,
        **task_defaults)
