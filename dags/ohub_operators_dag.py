from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.operators.bash_operator import BashOperator

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from custom_operators.empty_fallback import EmptyFallbackOperator
from custom_operators.file_from_wasb import FileFromWasbOperator
from ohub_dag_config import \
    default_args, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, export_bucket, \
    wasb_raw_container, wasb_export_container, \
    default_cluster_config, interval, one_day_ago, two_day_ago, wasb_conn_id, container_name, \
    ingest_task, delta_fuzzy_matching_tasks, create_cluster, terminate_cluster, postgres_config

default_args.update(
    {'start_date': datetime(2018, 6, 4)}
)
schema = 'operators'
cluster_name = "ohub_operators_{{ds}}"

with DAG('ohub_{}'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    operators_create_cluster = create_cluster('{}_create_clusters'.format(schema),
                                              default_cluster_config(cluster_name))
    operators_terminate_cluster = terminate_cluster('{}_terminate_cluster'.format(schema), cluster_name)

    empty_fallback = EmptyFallbackOperator(
        task_id='{}_empty_fallback'.format(schema),
        container_name='prod',
        file_path=wasb_raw_container.format(date=one_day_ago, schema=schema, channel='file_interface'),
        wasb_conn_id=wasb_conn_id)

    operators_file_interface_to_parquet = ingest_task(
        schema=schema,
        channel='file_interface',
        clazz="com.unilever.ohub.spark.tsv2parquet.file_interface.OperatorConverter",
        cluster_name=cluster_name
    )

    begin_fuzzy_matching = BashOperator(
        task_id='{}_start_fuzzy_matching'.format(schema),
        bash_command='echo "start fuzzy matching"',
    )

    matching_tasks = delta_fuzzy_matching_tasks(
        schema=schema,
        cluster_name=cluster_name,
        delta_match_py='dbfs:/libraries/name_matching/delta_operators.py',
        match_py='dbfs:/libraries/name_matching/match_operators.py',
        integrated_input=integrated_bucket.format(date=two_day_ago, fn=schema),
        ingested_input=ingested_bucket.format(date=one_day_ago, fn=schema, channel='*'),
    )

    end_fuzzy_matching = BashOperator(
        task_id='{}_end_fuzzy_matching'.format(schema),
        bash_command='echo "end fuzzy matching"',
    )

    join_operators = DatabricksSubmitRunOperator(
        task_id='{}_merge'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_fuzzy_matched_delta'.format(schema)),
                           '--entityInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_delta_left_overs'.format(schema)),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_delta_golden_records'.format(schema))] + postgres_config
        })

    combine_to_create_integrated = DatabricksSubmitRunOperator(
        task_id='{}_combine_to_create_integrated'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.OperatorCombining",
            'parameters': ['--integratedUpdated',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_fuzzy_matched_delta_integrated'.format(schema)),
                           '--newGolden',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_delta_golden_records'.format(schema)),
                           '--combinedOperators',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(schema))]
        }
    )

    update_golden_records = DatabricksSubmitRunOperator(
        task_id='{}_update_golden_records'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(schema)),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn=schema)] + postgres_config
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
            'parameters': ['--inputFile', integrated_bucket.format(date=one_day_ago, fn=schema),
                           '--outputFile', export_bucket.format(date=one_day_ago, fn=op_file),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=schema)] + postgres_config
        }
    )

    tmp_file = '/tmp/' + op_file

    operators_acm_from_wasb = FileFromWasbOperator(
        task_id='{}_acm_from_wasb'.format(schema),
        file_path=tmp_file,
        container_name=container_name,
        wasb_conn_id=wasb_conn_id,
        blob_name=wasb_export_container.format(date=one_day_ago, fn=op_file)
    )

    operators_ftp_to_acm = SFTPOperator(
        task_id='{}_ftp_to_acm'.format(schema),
        local_filepath=tmp_file,
        remote_filepath='/incoming/UFS_upload_folder/',
        ssh_conn_id='acm_sftp_ssh',
        operation=SFTPOperation.PUT)

    update_operators_table = DatabricksSubmitRunOperator(
        task_id='{}_update_table'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        notebook_task={'notebook_path': '/Users/tim.vancann@unilever.com/update_integrated_tables'}
    )

    empty_fallback >> operators_file_interface_to_parquet
    operators_create_cluster >> operators_file_interface_to_parquet >> begin_fuzzy_matching
    for t in matching_tasks['in']:
        begin_fuzzy_matching >> t
    for t in matching_tasks['out']:
        t >> end_fuzzy_matching
    end_fuzzy_matching >> join_operators >> combine_to_create_integrated
    combine_to_create_integrated >> update_golden_records >> update_operators_table >> operators_terminate_cluster
    update_golden_records >> operators_to_acm >> operators_terminate_cluster
    operators_to_acm >> operators_acm_from_wasb >> operators_ftp_to_acm
