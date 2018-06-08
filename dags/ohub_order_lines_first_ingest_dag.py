from datetime import datetime

from airflow import DAG

from ohub_dag_config import default_args, pipeline_without_matching

schema = 'orderlines'
clazz = 'OrderLine'

interval = '@once'
default_args.update(
    {'start_date': datetime(2018, 6, 3)}
)
cluster_name = "ohub_orderlines_initial_load_{{ds}}"

with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    pipeline_without_matching(
        schema=schema,
        cluster_name=cluster_name,
        clazz=clazz,
        acm_file_prefix='UFS_ORDERLINES')
