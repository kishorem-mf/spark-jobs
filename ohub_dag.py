from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, \
    DataProcSparkOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 20),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'pool': 'ohub-pool',
}

gs_jar_bucket = 'gs://ufs-prod/deployment/ohub2.0'
gs_data_input_bucket = 'gs://ufs-prod/data/raw/{}/**/*.csv'
gs_data_output_bucket = 'gs://ufs-prod/data/parquet/{}.parquet'

cluster_defaults = {
    'gcp_conn_id': 'airflow-sp',
    'cluster_name': 'ouniverse-new-leads',
    'project_id': 'ufs-prod',
    'region': 'global',  # has to be global due to bug in airflow dataproc code
    'jars': [f'{gs_jar_bucket}/spark-jobs-assembly-0.1.jar'],
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
     'input': 'OPERATORS'},
    {'class': 'OrderConverter',
     'input': 'ORDERS'},
    {'class': 'ContactPersonConverter',
     'input': 'CONTACTPERSONS'},
    {'class': 'ProductConverter',
     'input': 'PRODUCTS'},
]
with DAG('ohub_dag', default_args=default_args,
         schedule_interval="0 0 * * *") as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        num_workers=5,
        worker_machine_type='n1-standard-16',
        zone='europe-west4-c',
        **cluster_defaults)

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        **cluster_defaults)

    for task in csv_to_parquet:
        task_name = f"{task['input'].lower()}_to_csv"
        globals()[task_name] = DataProcSparkOperator(
            task_id=task_name,
            main_class=f"com.unilever.ohub.spark.tsv2parquet/{task['class']}",
            dataproc_spark_jars=cluster_defaults['jars'],
            cluster_name=cluster_defaults['cluster_name'],
            gcp_conn_id=cluster_defaults['gcp_conn_id'],
            arguments=[gs_data_input_bucket.format(task['input']),
                       gs_data_output_bucket.format(task['input'].lower())])
        create_cluster >> globals()[task_name]

    for task in to_acm:
        task_name = f"{task['input'].lower()}_to_acm"
        globals()[task_name] = DataProcSparkOperator(
            task_id=task_name,
            main_class=f"com.unilever.ohub.spark.acm/{task['class']}",
            dataproc_spark_jars=cluster_defaults['jars'],
            cluster_name=cluster_defaults['cluster_name'],
            gcp_conn_id=cluster_defaults['gcp_conn_id'],
            arguments=[gs_data_input_bucket.format(task['input']),
                       gs_data_output_bucket.format(task['input'].lower())])
        globals()[task_name] >> delete_cluster
