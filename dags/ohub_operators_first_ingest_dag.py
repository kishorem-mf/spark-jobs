from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.hooks.base_hook import BaseHook
from airflow.operators.subdag_operator import SubDagOperator

from custom_operators.databricks_functions import \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator, \
    DatabricksCreateClusterOperator
from custom_operators.file_from_wasb import FileFromWasbOperator
from operators_config import \
    default_args, \
    databricks_conn_id, \
    jar, egg, \
    raw_bucket, ingested_bucket, intermediate_bucket, integrated_bucket, export_bucket, \
    wasb_export_container, operator_country_codes

default_args.update(
    {'start_date': datetime(2018, 5, 17)}
)
interval = '@once'

utc_now = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
cluster_name = "ohub_operators_{}".format(utc_now)

with DAG('ohub_operators_first_ingest', default_args=default_args,
         schedule_interval=interval) as dag:

    create_cluster = DatabricksCreateClusterOperator(
        task_id='create_cluster',
        databricks_conn_id=databricks_conn_id,
        cluster_config={
            "cluster_name": cluster_name,
            "spark_version": "4.0.x-scala2.11",
            "node_type_id": "Standard_DS5_v2",
            "autoscale": {
                "min_workers": 4,
                "max_workers": 12
            },
            "autotermination_minutes": 30,
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
            },
        }
    )

    terminate_cluster = DatabricksTerminateClusterOperator(
        task_id='terminate_cluster',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id
    )

    postgres_connection = BaseHook.get_connection('postgres_channels')

    operators_file_interface_to_parquet = DatabricksSubmitRunOperator(
        task_id="operators_file_interface_to_parquet",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.tsv2parquet.file_interface.OperatorConverter",
            'parameters': ['--inputFile', raw_bucket.format(date='{{ds}}',
                                                            schema='operators',
                                                            channel='file_interface'),
                           '--outputFile', ingested_bucket.format(date='{{ds}}',
                                                                  fn='operators',
                                                                  channel='file_interface'),
                           '--strictIngestion', "false",
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    def matching_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
        sub_dag = DAG(
            '%s.%s' % (parent_dag_name, child_dag_name),
            schedule_interval=schedule_interval,
            start_date=start_date,
            default_args=default_args
        )
        for code in operator_country_codes:
            DatabricksSubmitRunOperator(
                dag=sub_dag,
                task_id='match_operators_{}'.format(code),
                cluster_name=cluster_name,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': 'dbfs:/libraries/name_matching/match_operators.py',
                    'parameters': ['--input_file', ingested_bucket.format(date='{{ds}}',
                                                                          fn='operators',
                                                                          channel='*'),
                                   '--output_path', intermediate_bucket.format(date='{{ds}}', fn='operators_matched'),
                                   '--country_code', code,
                                   '--threshold', '0.9']
                }
            )

        return sub_dag

    match_per_country = SubDagOperator(
        subdag=matching_sub_dag('ohub_operators_first_ingest', 'match_per_country', default_args['start_date'],
                                interval),
        task_id='match_per_country',
    )

    merge_operators = DatabricksSubmitRunOperator(
        task_id='join_operators',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            'parameters': ['--matchingInputFile', intermediate_bucket.format(date='{{ds}}', fn='operators_matched'),
                           '--entityInputFile', ingested_bucket.format(date='{{ds}}',
                                                                       fn='operators',
                                                                       channel='*'),
                           '--outputFile', integrated_bucket.format(date='{{ds}}', fn='operators'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    op_file = 'acm/UFS_OPERATORS_{{ds_nodash}}000000.csv'

    operators_to_acm = DatabricksSubmitRunOperator(
        task_id="operators_to_acm",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.OperatorAcmConverter",
            'parameters': ['--inputFile', integrated_bucket.format(date='{{ds}}', fn='operators'),
                           '--outputFile', export_bucket.format(date='{{ds}}', fn=op_file),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        }
    )

    tmp_file = '/tmp/' + op_file

    operators_acm_from_wasb = FileFromWasbOperator(
        task_id='operators_acm_from_wasb',
        file_path=tmp_file,
        container_name='prod',
        wasb_conn_id='azure_blob',
        blob_name=wasb_export_container.format(date='{{ds}}', fn=op_file)
    )

    operators_ftp_to_acm = SFTPOperator(
        task_id='operators_ftp_to_acm',
        local_filepath=tmp_file,
        remote_filepath='/incoming/temp/ohub_2_test/{}'.format(tmp_file.split('/')[-1]),
        ssh_conn_id='acm_sftp_ssh',
        operation=SFTPOperation.PUT
    )

    create_cluster >> operators_file_interface_to_parquet >> match_per_country
    match_per_country >> merge_operators >> operators_to_acm >> terminate_cluster
    operators_to_acm >> operators_acm_from_wasb >> operators_ftp_to_acm
