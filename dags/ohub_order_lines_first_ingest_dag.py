from datetime import datetime

from airflow import DAG

from ohub_dag_config import default_args, GenericPipeline, \
    SubPipeline, integrated_bucket, one_day_ago

schema = 'orderlines'
clazz = 'OrderLine'

interval = '@once'
default_args.update(
    {'start_date': datetime(2018, 6, 13)}
)
cluster_name = "ohub_orderlines_initial_load_{{ds}}"

with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    generic = (
        GenericPipeline(schema=schema, cluster_name=cluster_name, clazz=clazz)
            .has_export_to_acm(acm_schema_name='UFS_ORDERSLINES')
            .has_ingest_from_file_interface(deduplicate_on_concat_id=False,
                                            alternative_schema='orders',
                                            alternative_output_fn=integrated_bucket.format(date=one_day_ago, fn=schema))
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()

    ingest.last_task >> export.first_task

