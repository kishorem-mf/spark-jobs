from datetime import datetime

from airflow import DAG

from ohub_dag_config import default_args, initial_load_pipeline_without_matching

schema = 'orders'
clazz = 'Order'

interval = '@once'
default_args.update(
    {'start_date': datetime(2018, 6, 3)}
)
cluster_name = "ohub_orders_initial_load_{{ds}}"

with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    initial_load_pipeline_without_matching(schema, cluster_name, clazz)
