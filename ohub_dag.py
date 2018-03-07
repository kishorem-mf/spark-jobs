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
    'pool': 'ohub-pool',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

with DAG('ohub_dag', default_args=default_args,
         schedule_interval="0 * * * *") as dag:
    operators_to_datalake = BashOperator(
        task_id='operators_to_datalake',
        bash_command='echo "operators_to_datalake"')

    contactpersons_to_datalake = BashOperator(
        task_id='contactpersons_to_datalake',
        bash_command='echo "contactpersons_to_datalake"')

    orders_to_datalake = BashOperator(
        task_id='orders_to_datalake',
        bash_command='echo "orders_to_datalake"')

    products_to_datalake = BashOperator(
        task_id='products_to_datalake',
        bash_command='echo "products_to_datalake"')

    create_spark_cluster = BashOperator(
        task_id='create_spark_cluster',
        bash_command='echo "create_spark_cluster"')

    operators_to_parquet = BashOperator(
        task_id='operators_to_parquet',
        bash_command='echo "operators_to_parquet"')

    contactpersons_to_parquet = BashOperator(
        task_id='contactpersons_to_parquet',
        bash_command='echo "contactpersons_to_parquet"')

    orders_to_parquet = BashOperator(
        task_id='orders_to_parquet',
        bash_command='echo "orders_to_parquet"')

    products_to_parquet = BashOperator(
        task_id='products_to_parquet',
        bash_command='echo "products_to_parquet"')

    operators_deduplicate = BashOperator(
        task_id='operators_deduplicate',
        bash_command='echo "operators_deduplicate"')

    contactpersons_deduplicate = BashOperator(
        task_id='contactpersons_deduplicate',
        bash_command='echo "contactpersons_deduplicate"')

    orders_deduplicate = BashOperator(
        task_id='orders_deduplicate',
        bash_command='echo "orders_deduplicate"')

    products_deduplicate = BashOperator(
        task_id='products_deduplicate',
        bash_command='echo "products_deduplicate"')

    delete_spark_cluster = BashOperator(
        task_id='delete_spark_cluster',
        bash_command='echo "delete_spark_cluster"')

    operators_to_acm = BashOperator(
        task_id='operators_to_acm',
        bash_command='echo "operators_to_acm"')

    contactpersons_to_acm = BashOperator(
        task_id='contactpersons_to_acm',
        bash_command='echo "contactpersons_to_acm"')

    orders_to_acm = BashOperator(
        task_id='orders_to_acm',
        bash_command='echo "orders_to_acm"')

    products_to_acm = BashOperator(
        task_id='products_to_acm',
        bash_command='echo "products_to_acm"')

operators_to_datalake >> create_spark_cluster
contactpersons_to_datalake >> create_spark_cluster
orders_to_datalake >> create_spark_cluster
products_to_datalake >> create_spark_cluster

create_spark_cluster >> operators_to_parquet >> \
 operators_deduplicate >> delete_spark_cluster
create_spark_cluster >> contactpersons_to_parquet >> \
 contactpersons_deduplicate >> delete_spark_cluster
create_spark_cluster >> orders_to_parquet >> \
 orders_deduplicate >> delete_spark_cluster
create_spark_cluster >> products_to_parquet >> \
 products_deduplicate >> delete_spark_cluster

operators_deduplicate >> operators_to_acm
contactpersons_deduplicate >> contactpersons_to_acm
orders_deduplicate >> orders_to_acm
products_deduplicate >> products_to_acm
