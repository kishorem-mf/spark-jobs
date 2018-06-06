from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.operators.bash_operator import BashOperator

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from custom_operators.file_from_wasb import FileFromWasbOperator
from ohub_dag_config import \
    default_args, container_name, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, \
    export_bucket, \
    wasb_export_container, create_cluster, terminate_cluster, default_cluster_config, \
    postgres_config, ingest_task, fuzzy_matching_tasks

interval = '@once'
schemas = {
    'products': 'Product',
    'orders': 'Order',
    'order_lines': 'OrderLine',
}
for schema, clazz in schemas.items():
    default_args.update(
        {
            'start_date': datetime(2018, 6, 3),
            'pool': 'ohub_{}_pool'.format(schema)
        }
    )
    cluster_name = "ohub_{}_initial_load_{{ds}}".format(schema)

    with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
            schedule_interval=interval) as dag:
        cluster_up = create_cluster('{}_create_clusters'.format(schema),
                                    default_cluster_config(cluster_name))
        cluster_down = terminate_cluster('{}_terminate_cluster'.format(schema), cluster_name)

        file_interface_to_parquet = ingest_task(
            schema=schema,
            channel='file_interface',
            clazz="com.unilever.ohub.spark.tsv2parquet.file_interface.{}Converter".format(clazz),
            field_separator=u"\u2030",
            cluster_name=cluster_name
        )

        acm_file = 'acm/UFS_{}_{{ds_nodash}}000000.csv'.format(schema)

        convert_to_acm = DatabricksSubmitRunOperator(
            task_id="{}_to_acm".format(schema),
            cluster_name=cluster_name,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'jar': jar}
            ],
            spark_jar_task={
                'main_class_name': "com.unilever.ohub.spark.acm.{}AcmConverter".format(clazz),
                'parameters': ['--inputFile', integrated_bucket.format(date='{{ds}}', fn=schema),
                            '--outputFile', export_bucket.format(date='{{ds}}', fn=acm_file)] + postgres_config
            }
        )

        tmp_file = '/tmp/' + acm_file

        acm_from_wasb = FileFromWasbOperator(
            task_id='{}_acm_from_wasb'.format(schema),
            file_path=tmp_file,
            container_name=container_name,
            wasb_conn_id='azure_blob',
            blob_name=wasb_export_container.format(date='{{ds}}', fn=acm_file)
        )

        ftp_to_acm = SFTPOperator(
            task_id='{}_ftp_to_acm'.format(schema),
            local_filepath=tmp_file,
            remote_filepath='/incoming/temp/ohub_2_test/{}'.format(tmp_file.split('/')[-1]),
            ssh_conn_id='acm_sftp_ssh',
            operation=SFTPOperation.PUT
        )

        cluster_up >> file_interface_to_parquet >> convert_to_acm >> cluster_down
        convert_to_acm >> acm_from_wasb >> ftp_to_acm
