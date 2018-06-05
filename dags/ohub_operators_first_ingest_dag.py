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

default_args.update(
    {'start_date': datetime(2018, 5, 17)}
)
interval = '@once'
schema = 'operators'
cluster_name = "ohub_operators_initial_load_{{ds}}"

with DAG('ohub_operators_first_ingest', default_args=default_args,
         schedule_interval=interval) as dag:
    operators_create_cluster = create_cluster('{}_create_clusters'.format(schema),
                                              default_cluster_config(cluster_name))
    operators_terminate_cluster = terminate_cluster('{}_terminate_cluster'.format(schema), cluster_name)

    operators_file_interface_to_parquet = ingest_task(
        schema=schema,
        channel='file_interface',
        clazz="com.unilever.ohub.spark.tsv2parquet.file_interface.OperatorConverter",
        field_separator=u"\u2030",
        cluster_name=cluster_name
    )

    begin_fuzzy_matching = BashOperator(
        task_id='{}_start_fuzzy_matching'.format(schema),
        bash_command='echo "start fuzzy matching"',
    )

    matching_tasks = fuzzy_matching_tasks(
        schema=schema,
        cluster_name=cluster_name,
        match_py='dbfs:/libraries/name_matching/match_operators.py',
        ingested_input=ingested_bucket.format(date='{{ds}}', fn='operators', channel='*')
    )

    end_fuzzy_matching = BashOperator(
        task_id='{}_end_fuzzy_matching'.format(schema),
        bash_command='echo "end fuzzy matching"',
    )

    merge_operators = DatabricksSubmitRunOperator(
        task_id='{}_join'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date='{{ds}}', fn='{}_matched'.format(schema)),
                           '--entityInputFile', ingested_bucket.format(date='{{ds}}', fn=schema, channel='*'),
                           '--outputFile', integrated_bucket.format(date='{{ds}}', fn=schema)] + postgres_config
        }
    )

    op_file = 'acm/UFS_OPERATORS_{{ds_nodash}}000000.csv'

    operators_to_acm = DatabricksSubmitRunOperator(
        task_id="{}_to_acm".format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.OperatorAcmConverter",
            'parameters': ['--inputFile', integrated_bucket.format(date='{{ds}}', fn=schema),
                           '--outputFile', export_bucket.format(date='{{ds}}', fn=op_file)] + postgres_config
        }
    )

    tmp_file = '/tmp/' + op_file

    operators_acm_from_wasb = FileFromWasbOperator(
        task_id='{}_acm_from_wasb'.format(schema),
        file_path=tmp_file,
        container_name=container_name,
        wasb_conn_id='azure_blob',
        blob_name=wasb_export_container.format(date='{{ds}}', fn=op_file)
    )

    operators_ftp_to_acm = SFTPOperator(
        task_id='{}_ftp_to_acm'.format(schema),
        local_filepath=tmp_file,
        remote_filepath='/incoming/temp/ohub_2_test/{}'.format(tmp_file.split('/')[-1]),
        ssh_conn_id='acm_sftp_ssh',
        operation=SFTPOperation.PUT
    )

    operators_create_cluster >> operators_file_interface_to_parquet >> begin_fuzzy_matching
    for t in matching_tasks:
        begin_fuzzy_matching >> t >> end_fuzzy_matching
    end_fuzzy_matching >> merge_operators >> operators_to_acm >> operators_terminate_cluster
    operators_to_acm >> operators_acm_from_wasb >> operators_ftp_to_acm
