from datetime import datetime

from airflow import DAG
from airflow.models import Variable

from custom_operators.databricks_functions import \
    DatabricksCreateClusterOperator, \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 2, 6),
    'email': ['timvancann@godatadriven.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

wasb_root_bucket = 'wasbs://prod@ulohub2storedevne.blob.core.windows.net/data/'
data_bucket = wasb_root_bucket + 'ouniverse'

cluster_name = 'ouniverse_new_leads'
databricks_conn_id = 'databricks_azure'

cluster_config = {
    'cluster_name': cluster_name,
    "spark_version": "3.5.x-scala2.11",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 16
}

with DAG('new_leads', default_args=default_args,
         schedule_interval='@once') as dag:
    create_cluster = DatabricksCreateClusterOperator(
        task_id='create_cluster',
        cluster_config=cluster_config,
        databricks_conn_id=databricks_conn_id,
        polling_period_seconds=10
    )

    delete_cluster = DatabricksTerminateClusterOperator(
        task_id='destroy_cluster',
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id
    )

    phase_one = DatabricksSubmitRunOperator(
        task_id="phase_one",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/ouniverse-prioritisation-assembly-1.0.0-SNAPSHOT.jar'}
        ],
        spark_jar_task={
            'main_class_name': f"com.unilever.ouniverse.leads.OperatorMapsSearcher",
            'parameters': ['--operators', f'{data_bucket}/input/phase_I_NZ_sample.csv',
                           '--outputpath', f'{data_bucket}/output/phaseI_output',
                           '--apiKey', Variable.get('google_api_key')]
        }
    )

    phase_two_grid = DatabricksSubmitRunOperator(
        task_id="phase_two_grid",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/ouniverse-prioritisation-assembly-1.0.0-SNAPSHOT.jar'}
        ],
        spark_jar_task={
            'main_class_name': f"com.unilever.ouniverse.leads.GridSearcher",
            'parameters': ['--leads', f'{data_bucket}/input/Phase_II_Input_NZ_sample.csv',
                           '--outputpath', f'{data_bucket}/output/phaseIIa_output',
                           '--apiKey', Variable.get('google_api_key')]
        }
    )

    phase_two_ids = DatabricksSubmitRunOperator(
        task_id="phase_two_ids",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/ouniverse-prioritisation-assembly-1.0.0-SNAPSHOT.jar'}
        ],
        spark_jar_task={
            'main_class_name': f"com.unilever.ouniverse.leads.PlaceIdSearcher",
            'parameters': ['--places', f'{data_bucket}/output/phaseIIa_output',
                           '--idColumn', 'placeId',
                           '--fileType', 'parquet',
                           '--outputpath', f'{data_bucket}/output/phaseIIb_output',
                           '--apiKey', Variable.get('google_api_key')]
        }
    )

    prioritize = DatabricksSubmitRunOperator(
        task_id="prioritise_leads",
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/ouniverse-prioritisation-assembly-1.0.0-SNAPSHOT.jar'}
        ],
        spark_jar_task={
            'main_class_name': f"com.unilever.ouniverse.prioritisation.PrioritizeLeads",
            'parameters': ['--operators', f'{data_bucket}/output/phaseI_output',
                           '--places', f'{data_bucket}/output/phaseIIb_output',
                           '--leads', f'{data_bucket}/input/cities.csv',
                           '--priorities', f'{data_bucket}/input/priorities.csv',
                           '--outputpath', f'{data_bucket}/output/phaseIII_output']
        }
    )

    create_cluster >> phase_one >> prioritize
    create_cluster >> phase_two_grid >> phase_two_ids >> prioritize
    prioritize >> delete_cluster
