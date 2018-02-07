from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 12, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('ohub_dag', default_args=default_args, schedule_interval="0 * * * *")

start_fetch_data = DummyOperator(task_id="start_fetch_data", dag=dag)

operators_to_datalake = BashOperator(
    task_id='operators_to_datalake',
    bash_command='echo "operators_to_datalake"',
    dag=dag)

contactpersons_to_datalake = BashOperator(
    task_id='contactpersons_to_datalake',
    bash_command='echo "contactpersons_to_datalake"',
    dag=dag)

orders_to_datalake = BashOperator(
    task_id='orders_to_datalake',
    bash_command='echo "orders_to_datalake"',
    dag=dag)

products_to_datalake = BashOperator(
    task_id='products_to_datalake',
    bash_command='echo "products_to_datalake"',
    dag=dag)

create_spark_cluster = BashOperator(
    task_id='create_spark_cluster',
    bash_command='echo "create_spark_cluster"',
    dag=dag)

start_parquet_files = DummyOperator(task_id="start_parquet_files", dag=dag)

operators_to_parquet = BashOperator(
    task_id='operators_to_parquet',
    bash_command='echo "operators_to_parquet"',
    dag=dag)

contactpersons_to_parquet = BashOperator(
    task_id='contactpersons_to_parquet',
    bash_command='echo "contactpersons_to_parquet"',
    dag=dag)

orders_to_parquet = BashOperator(
    task_id='orders_to_parquet',
    bash_command='echo "orders_to_parquet"',
    dag=dag)

products_to_parquet = BashOperator(
    task_id='products_to_parquet',
    bash_command='echo "products_to_parquet"',
    dag=dag)

start_deduplicate = DummyOperator(task_id="start_deduplicate", dag=dag)

operators_deduplicate = BashOperator(
    task_id='operators_deduplicate',
    bash_command='echo "operators_deduplicate"',
    dag=dag)

contactpersons_deduplicate = BashOperator(
    task_id='contactpersons_deduplicate',
    bash_command='echo "contactpersons_deduplicate"',
    dag=dag)

orders_deduplicate = BashOperator(
    task_id='orders_deduplicate',
    bash_command='echo "orders_deduplicate"',
    dag=dag)

products_deduplicate = BashOperator(
    task_id='products_deduplicate',
    bash_command='echo "products_deduplicate"',
    dag=dag)

delete_spark_cluster = BashOperator(
    task_id='delete_spark_cluster',
    bash_command='echo "delete_spark_cluster"',
    dag=dag)

start_acm = DummyOperator(task_id="start_acm", dag=dag)

operators_to_acm = BashOperator(
    task_id='operators_to_acm',
    bash_command='echo "operators_to_acm"',
    dag=dag)

contactpersons_to_acm = BashOperator(
    task_id='contactpersons_to_acm',
    bash_command='echo "contactpersons_to_acm"',
    dag=dag)

orders_to_acm = BashOperator(
    task_id='orders_to_acm',
    bash_command='echo "orders_to_acm"',
    dag=dag)

products_to_acm = BashOperator(
    task_id='products_to_acm',
    bash_command='echo "products_to_acm"',
    dag=dag)

operators_to_datalake.set_upstream(start_fetch_data)
operators_to_datalake.set_downstream(start_parquet_files)
contactpersons_to_datalake.set_upstream(start_fetch_data)
contactpersons_to_datalake.set_downstream(start_parquet_files)
orders_to_datalake.set_upstream(start_fetch_data)
orders_to_datalake.set_downstream(start_parquet_files)
products_to_datalake.set_upstream(start_fetch_data)
products_to_datalake.set_downstream(start_parquet_files)

create_spark_cluster.set_downstream(start_parquet_files)

operators_to_parquet.set_upstream(start_parquet_files)
operators_to_parquet.set_downstream(start_deduplicate)
contactpersons_to_parquet.set_upstream(start_parquet_files)
contactpersons_to_parquet.set_downstream(start_deduplicate)
orders_to_parquet.set_upstream(start_parquet_files)
orders_to_parquet.set_downstream(start_deduplicate)
products_to_parquet.set_upstream(start_parquet_files)
products_to_parquet.set_downstream(start_deduplicate)

operators_deduplicate.set_upstream(start_deduplicate)
operators_deduplicate.set_downstream(start_acm)
contactpersons_deduplicate.set_upstream(start_deduplicate)
contactpersons_deduplicate.set_downstream(start_acm)
orders_deduplicate.set_upstream(start_deduplicate)
orders_deduplicate.set_downstream(start_acm)
products_deduplicate.set_upstream(start_deduplicate)
products_deduplicate.set_downstream(start_acm)

delete_spark_cluster.set_upstream(start_acm)

operators_to_acm.set_upstream(start_acm)
contactpersons_to_acm.set_upstream(start_acm)
orders_to_acm.set_upstream(start_acm)
products_to_acm.set_upstream(start_acm)
