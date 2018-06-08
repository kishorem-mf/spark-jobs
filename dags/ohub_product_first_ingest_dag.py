from datetime import datetime

from airflow import DAG

from ohub_dag_config import \
    default_args, create_cluster, terminate_cluster, ingest_task, small_cluster_config, \
    acm_initial_load_convert_and_move

schema = 'products'
clazz = 'Product'

interval = '@once'
default_args.update(
    {'start_date': datetime(2018, 6, 3)}
)
cluster_name = "ohub_products_initial_load_{{ds}}"

with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    cluster_up = create_cluster('{}_create_clusters'.format(schema), small_cluster_config(cluster_name))
    cluster_down = terminate_cluster('{}_terminate_cluster'.format(schema), cluster_name)

    file_interface_to_parquet = ingest_task(
        schema=schema,
        channel='file_interface',
        clazz="com.unilever.ohub.spark.tsv2parquet.file_interface.{}Converter".format(clazz),
        field_separator=u"\u2030",
        cluster_name=cluster_name
    )

    convert_to_acm = acm_initial_load_convert_and_move(
        schema=schema,
        cluster_name=cluster_name,
        clazz=clazz
    )

    cluster_up >> file_interface_to_parquet >> convert_to_acm >> cluster_down
