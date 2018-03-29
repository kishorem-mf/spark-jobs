from datetime import datetime

from airflow import DAG
from airflow.models import Variable

from custom_operators.databricks_functions import \
    DatabricksCreateClusterOperator, \
    DatabricksTerminateClusterOperator, \
    DatabricksSubmitRunOperator, DatabricksStartClusterOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 11),
    'email': ['timvancann@godatadriven.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

wasb_root_bucket = 'wasbs://prod@ulohub2storedevne.blob.core.windows.net/data/'
data_input_bucket = wasb_root_bucket + 'raw/ouniverse/'
data_output_bucket = wasb_root_bucket + 'processed/ouniverse/'

cluster_name = 'ouniverse'
cluster_id = '0320-064521-hex234'
databricks_conn_id = 'databricks_azure'

with DAG('new_leads', default_args=default_args,
         schedule_interval='@once') as dag:
    create_cluster = DatabricksStartClusterOperator(
        task_id='create_cluster',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        polling_period_seconds=10
    )

    delete_cluster = DatabricksTerminateClusterOperator(
        task_id='terminate_cluster',
        cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id
    )

    phase_one = DatabricksSubmitRunOperator(
        task_id="phase_one",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/ouniverse-prioritisation-assembly-1.0.0-SNAPSHOT.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ouniverse.leads.OperatorMapsSearcher",
            'parameters': ['--operators', data_input_bucket + '/operators/{{ds}}/*.csv',
                           '--outputpath', data_output_bucket + '/operators/{{ds}}/operators.parquet',
                           '--apiKey', Variable.get('google_api_key')]
        }
    )

    phase_two_grid = DatabricksSubmitRunOperator(
        task_id="phase_two_grid",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/ouniverse-prioritisation-assembly-1.0.0-SNAPSHOT.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ouniverse.leads.GridSearcher",
            'parameters': ['--leads', data_input_bucket + '/grid/{{ds}}/*.csv',
                           '--outputpath', data_output_bucket + '/grid/{{ds}}/grid.parquet',
                           '--apiKey', Variable.get('google_api_key')]
        }
    )

    phase_two_ids = DatabricksSubmitRunOperator(
        task_id="phase_two_ids",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/ouniverse-prioritisation-assembly-1.0.0-SNAPSHOT.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ouniverse.leads.PlaceIdSearcher",
            'parameters': ['--places', data_output_bucket + '/grid/{{ds}}/*',
                           '--idColumn', 'placeId',
                           '--fileType', 'parquet',
                           '--outputpath', data_output_bucket + '/place_details/{{ds}}/details.parquet',
                           '--apiKey', Variable.get('google_api_key')]
        }
    )

    prioritize = DatabricksSubmitRunOperator(
        task_id="prioritise_leads",
        existing_cluster_id=cluster_id,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': 'dbfs:/libraries/ouniverse-prioritisation-assembly-1.0.0-SNAPSHOT.jar'}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ouniverse.prioritisation.PrioritizeLeads",
            'parameters': ['--operators', data_output_bucket + '/operators/{{ds}}/*',
                           '--places', data_output_bucket + '/place_detail/{{ds}}/*',
                           '--leads', data_output_bucket + '/cities.csv',
                           '--priorities', data_output_bucket + '/priorities.csv',
                           '--outputpath', data_output_bucket + '/leads/{{ds}}/leads.parquet']
        }
    )

    create_cluster >> phase_one >> prioritize
    create_cluster >> phase_two_grid >> phase_two_ids >> prioritize
    prioritize >> delete_cluster
