from datetime import datetime

from airflow import DAG

from ohub_dag_config import default_args, GenericPipeline, \
    SubPipeline, integrated_bucket, one_day_ago, DagConfig

default_args.update(
    {'start_date': datetime(2018, 6, 13)}
)

entity = 'orderlines'
dag_config = DagConfig(entity, is_delta=False)
clazz = 'OrderLine'

with DAG(dag_config.dag_id, default_args=default_args, schedule_interval=dag_config.schedule) as dag:
    generic = (
        GenericPipeline(dag_config, class_prefix=clazz)
            .has_export_to_acm(acm_schema_name='UFS_ORDERSLINES')
            .has_ingest_from_file_interface(deduplicate_on_concat_id=False,
                                            alternative_schema='orders',
                                            alternative_output_fn=integrated_bucket.format(date=one_day_ago, fn=entity))
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()

    ingest.last_task >> export.first_task

