from datetime import datetime, timedelta

from airflow import DAG

from airflow.hooks.base_hook import BaseHook

from config import email_addresses
from custom_operators.databricks_functions import \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator, DatabricksStartClusterOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 7),
    'email': email_addresses,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'pool': 'ohub-pool',
}

wasb_root_bucket = 'wasbs://prod@ulohub2storedevne.blob.core.windows.net/data/'
data_input_bucket = wasb_root_bucket + 'raw/{}/**/*.csv'
data_output_bucket = wasb_root_bucket + 'parquet/{}.parquet'

csv_to_parquet = [
    {'class': 'OperatorConverter',
     'input': 'OPERATORS'},
    {'class': 'OrderConverter',
     'input': 'ORDERS'},
    {'class': 'ContactPersonConverter',
     'input': 'CONTACTPERSONS'},
    {'class': 'ProductConverter',
     'input': 'PRODUCTS'},
]

to_acm = [
    {'class': 'OperatorConverter',
     'output': 'operators',
     'input': 'operators_merged'},
    {'class': 'OrderConverter',
     'output': 'orders',
     'input': 'orders_merged'},
    {'class': 'ContactPersonConverter',
     'output': 'contactpersons',
     'input': 'contactpersons_merged_2'},
    {'class': 'ProductConverter',
     'output': 'products',
     'input': 'products'},
]

cluster_name = 'ohub_basic'
cluster_id = '0314-131901-shalt605'

databricks_conn_id = 'databricks_azure'

cluster_config = {
    'cluster_name': cluster_name,
    "spark_version": "3.5.x-scala2.11",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 16
}
with DAG('ohub_dag', default_args=default_args,
         schedule_interval="@once") as dag:
    create_cluster = DatabricksStartClusterOperator(
        task_id='start_cluster',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id
    )

    delete_cluster = DatabricksTerminateClusterOperator(
        task_id='terminate_cluster',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id
    )

    postgres_connection = BaseHook.get_connection('postgres_channels')

    for task in csv_to_parquet:
        task_name = "{}_to_parquet".format(task['input'].lower())
        globals()[task_name] = DatabricksSubmitRunOperator(
            task_id=task_name,
            existing_cluster_id=cluster_id,
            databricks_conn_id=databricks_conn_id,
            polling_period_seconds=10,
            libraries=[
                {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
            ],
            spark_jar_task={
                'main_class_name': "com.unilever.ohub.spark.tsv2parquet.{}".format(task['class']),
                'parameters': ['--inputFile', data_input_bucket.format(task['input']),
                               '--outputFile', data_output_bucket.format(task['output'].lower()),
                               '--postgressUrl', postgres_connection.host,
                               '--postgressUsername', postgres_connection.login,
                               '--postgressPassword', postgres_connection.password,
                               '--postgressDB', postgres_connection.schema]
            }
        )

        create_cluster >> globals()[task_name]

    for task in to_acm:
        task_name = "{}_to_acm".format(task['output'].lower())
        globals()[task_name] = DatabricksSubmitRunOperator(
            task_id=task_name,
            existing_cluster_id=cluster_id,
            databricks_conn_id=databricks_conn_id,
            polling_period_seconds=10,
            libraries=[
                {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
            ],
            spark_jar_task={
                'main_class_name': "com.unilever.ohub.spark.tsv2parquet.{}".format(task['class']),
                'parameters': ['--inputFile', data_input_bucket.format(task['input']),
                               '--outputFile', data_output_bucket.format(task['output']) + '_acm',
                               '--postgressUrl', postgres_connection.host,
                               '--postgressUsername', postgres_connection.login,
                               '--postgressPassword', postgres_connection.password,
                               '--postgressDB', postgres_connection.schema]
            })
        globals()[task_name] >> delete_cluster

    for task in to_acm:
        task_name = "{}_ftp_to_acm".format(task['output'].lower())
        globals()[task_name] = SFTPOperator(
            task_id=task_name,
            local_filepath=data_output_bucket.format(task['output']) + '_acm',
            remote_filepath='/incoming/UFS_upload_folder/,
            ssh_conn_id='acm_sftp_ssh',
            operation=SFTPOperation.PUT)

    match_operators = DatabricksSubmitRunOperator(
        task_id='match_operators',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        polling_period_seconds=60,
        libraries=[
            {'egg': 'dbfs:/libraries/string_matching.egg'}
        ],
        spark_python_task={
            'python_file': 'dbfs:/libraries/match_operators.py',
            'parameters': ['--input_file', data_output_bucket.format('operators'),
                           '--output_path', data_output_bucket.format('operators_matched')]
        }
    )

    persistent_uuid = DatabricksSubmitRunOperator(
        task_id='persistent_uuid',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'egg': 'dbfs:/libraries/string_matching.egg'}
        ],
        spark_python_task={
            'python_file': 'dbfs:/libraries/join_new_operators_with_persistent_uuid.py',
            'parameters': [
                '--current_operators_path', data_output_bucket.format('OPERATORS'),
                '--new_operators_path', data_output_bucket.format('operators_matched'),
                '--output_path', data_output_bucket.format('operators_uuid'),
                '--country_code', 'all',
                '--threshold', '0.8',
            ]
        }
    )

    merge_operators = DatabricksSubmitRunOperator(
        task_id='merge_operators',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMerging",
            'parameters': ['--matchingInputFile', data_output_bucket.format('operators_uuid'),
                           '--operatorInputFile', data_input_bucket.format('OPERATORS'),
                           '--outputFile', data_output_bucket.format('operators_merged'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        })

    operators_to_parquet >> match_operators >> persistent_uuid >> merge_operators >> operators_to_acm >> operators_ftp_to_acm

    merge_contactpersons1 = DatabricksSubmitRunOperator(
        task_id='merge_contactpersons_1',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMerging",
            'parameters': ['--inputFile', data_output_bucket.format('contactpersons'),
                           '--outputFile', data_output_bucket.format('contactpersons_merged_1'),
                           '--postgressUrl', postgres_connection.host,
                           '--postgressUsername', postgres_connection.login,
                           '--postgressPassword', postgres_connection.password,
                           '--postgressDB', postgres_connection.schema]
        })

    merge_operators >> merge_contactpersons1
    contactpersons_to_parquet >> merge_contactpersons1

    merge_contactpersons2 = DatabricksSubmitRunOperator(
        task_id='merge_contactpersons_2',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMerging2",
            'parameters': ['--matchingInputFile', data_output_bucket.format('contactpersons_merged_1'),
                           '--operatorInputFile', data_output_bucket.format('operators_merged'),
                           '--outputFile', data_output_bucket.format('contactpersons_merged_2')]
        })

    merge_contactpersons1 >> merge_contactpersons2 >> contactpersons_to_acm

    merge_orders = DatabricksSubmitRunOperator(
        task_id='merge_orders',
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/spark-jobs-assembly-0.1.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OrderMerging",
            'parameters': [data_output_bucket.format('contactpersons_merged_2'),
                           data_output_bucket.format('operators_merged'),
                           data_output_bucket.format('orders'),
                           data_output_bucket.format('orders_merged')]
        })

    merge_operators >> merge_orders >> globals()['orders_to_acm']
    merge_contactpersons2 >> merge_orders
    orders_to_parquet >> merge_orders

    products_to_parquet >> products_to_acm
