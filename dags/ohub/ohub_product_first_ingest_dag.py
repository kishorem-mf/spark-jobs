from datetime import datetime

from airflow import DAG

from custom_operators.wasb_copy import WasbCopyOperator
from ohub.ohub_dag_config import default_args, GenericPipeline, SubPipeline, one_day_ago, DagConfig, \
    wasb_integrated_container, http_intermediate_container, small_cluster_config

default_args.update(
    {'start_date': datetime(2018, 6, 13)}
)

entity = 'products'
dag_config = DagConfig(entity, is_delta=False)
clazz = 'Product'

with DAG(dag_config.dag_id, default_args=default_args, schedule_interval=dag_config.schedule) as dag:
    generic = (
        GenericPipeline(dag_config,
                        class_prefix=clazz,
                        cluster_config=small_cluster_config(dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='PRODUCTS')
            .has_export_to_dispatcher_db(dispatcher_schema_name='ORDER_PRODUCTS')
            .has_ingest_from_file_interface()
    )

    ingest: SubPipeline = generic.construct_ingest_pipeline()
    export: SubPipeline = generic.construct_export_pipeline()

    copy = WasbCopyOperator(
        task_id='copy_to_integrated',
        wasb_conn_id='azure_blob',
        container_name='prod',
        blob_name=wasb_integrated_container.format(date=one_day_ago, fn=entity),
        copy_source=http_intermediate_container.format(storage_account='ulohub2storedevne',
                                                       container='prod',
                                                       date=one_day_ago,
                                                       fn=f'{entity}_gathered')
    )

    ingest.last_task >> copy >> export.first_task
