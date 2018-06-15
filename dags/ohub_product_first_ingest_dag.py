from datetime import datetime

from airflow import DAG

from ohub_dag_config import default_args, pipeline_without_matching

schema = 'products'
clazz = 'Product'

interval = '@once'
default_args.update(
    {'start_date': datetime(2018, 6, 13)}
)
cluster_name = "ohub_products_initial_load_{{ds}}"

with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    pipeline_without_matching(
        schema=schema,
        cluster_name=cluster_name,
        clazz=clazz,
        acm_file_prefix='UFS_PRODUCTS')
