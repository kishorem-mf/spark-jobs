from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, \
    DataProcSparkOperator, DataProcPySparkOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 7),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'pool': 'ohub-pool',
}

gs_deployment = 'gs://ufs-prod/deployment/'
gs_jar_bucket = f'{gs_deployment}/ohub2.0'
gs_py_bucket = f'{gs_deployment}/name-matching'
gs_data_input_bucket = 'gs://ufs-prod/data/raw/{}/**/*.csv'
gs_data_output_bucket = 'gs://ufs-prod/data/parquet/{}.parquet'
jars = [f'{gs_jar_bucket}/spark-jobs-assembly-0.1.jar']
gs_init_scripts_bucket = 'gs://ufs-prod/jobs/dataproc-init'

cluster_defaults = {
    'gcp_conn_id': 'airflow-sp',
    'cluster_name': 'ouniverse-new-leads',
    'project_id': 'ufs-prod',
    'region': 'global',  # has to be global due to bug in airflow dataproc code
}

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
with DAG('ohub_dag', default_args=default_args,
         schedule_interval="@once") as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        num_workers=5,
        worker_machine_type='n1-standard-16',
        zone='europe-west4-c',
        init_actions_uris=[f'{gs_init_scripts_bucket}/ufs-conda.sh'],
        **cluster_defaults)

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        **cluster_defaults)

    for task in csv_to_parquet:
        task_name = f"{task['input'].lower()}_to_parquet"
        globals()[task_name] = DataProcSparkOperator(
            task_id=task_name,
            main_class=f"com.unilever.ohub.spark.tsv2parquet.{task['class']}",
            dataproc_spark_jars=jars,
            cluster_name=cluster_defaults['cluster_name'],
            gcp_conn_id=cluster_defaults['gcp_conn_id'],
            arguments=[gs_data_input_bucket.format(task['input']),
                       gs_data_output_bucket.format(task['input'].lower())])
        create_cluster >> globals()[task_name]

    for task in to_acm:
        task_name = f"{task['output'].lower()}_to_acm"
        globals()[task_name] = DataProcSparkOperator(
            task_id=task_name,
            main_class=f"com.unilever.ohub.spark.acm.{task['class']}",
            dataproc_spark_jars=jars,
            cluster_name=cluster_defaults['cluster_name'],
            gcp_conn_id=cluster_defaults['gcp_conn_id'],
            arguments=[gs_data_input_bucket.format(task['input']),
                       gs_data_output_bucket.format(task['output']) + '_acm'])
        globals()[task_name] >> delete_cluster

    match_operators = DataProcPySparkOperator(
        task_id='match_operators',
        main=f'{gs_py_bucket}/match_operators.py',
        pyfiles=[f'{gs_py_bucket}/dist/string_matching.egg'],
        cluster_name=cluster_defaults['cluster_name'],
        gcp_conn_id=cluster_defaults['gcp_conn_id'],
        arguments=['--input_file', gs_data_output_bucket.format('operators'),
                   '--output_path', gs_data_output_bucket.format('operators_matched')])

    merge_operators = DataProcSparkOperator(
        task_id='merge_operators',
        main_class='com.unilever.ohub.spark.merging.OperatorMerging',
        dataproc_spark_jars=jars,
        cluster_name=cluster_defaults['cluster_name'],
        gcp_conn_id=cluster_defaults['gcp_conn_id'],
        arguments=[gs_data_output_bucket.format('operators_matched'),
                   gs_data_input_bucket.format('OPERATORS'),
                   gs_data_output_bucket.format('operators_merged')])

    globals()['operators_to_parquet'] >> match_operators >> merge_operators >> globals()['operators_to_acm']

    merge_contactpersons1 = DataProcSparkOperator(
        task_id='merge_contactpersons_1',
        main_class='com.unilever.ohub.spark.merging.ContactPersonMerging1',
        dataproc_spark_jars=jars,
        cluster_name=cluster_defaults['cluster_name'],
        gcp_conn_id=cluster_defaults['gcp_conn_id'],
        arguments=[gs_data_output_bucket.format('contactpersons'),
                   gs_data_output_bucket.format('contactpersons_merged_1')])

    merge_operators >> merge_contactpersons1
    globals()['contactpersons_to_parquet'] >> merge_contactpersons1

    merge_contactpersons2 = DataProcSparkOperator(
        task_id='merge_contactpersons_2',
        main_class='com.unilever.ohub.spark.merging.ContactPersonMerging2',
        dataproc_spark_jars=jars,
        cluster_name=cluster_defaults['cluster_name'],
        gcp_conn_id=cluster_defaults['gcp_conn_id'],
        arguments=[gs_data_output_bucket.format('contactpersons_merged_1'),
                   gs_data_output_bucket.format('operators_merged'),
                   gs_data_output_bucket.format('contactpersons_merged_2')])

    merge_contactpersons1 >> merge_contactpersons2 >> globals()['contactpersons_to_acm']

    merge_orders = DataProcSparkOperator(
        task_id='merge_orders',
        main_class='com.unilever.ohub.spark.merging.OrderMerging',
        dataproc_spark_jars=jars,
        cluster_name=cluster_defaults['cluster_name'],
        gcp_conn_id=cluster_defaults['gcp_conn_id'],
        arguments=[gs_data_output_bucket.format('contactpersons_merged'),
                   gs_data_output_bucket.format('operators_merged'),
                   gs_data_output_bucket.format('contactpersons_merged')])

    merge_operators >> merge_orders >> globals()['orders_to_acm']
    merge_contactpersons2 >> merge_orders
    globals()['orders_to_parquet'] >> merge_orders

    globals()['products_to_parquet'] >> globals()['products_to_acm']
