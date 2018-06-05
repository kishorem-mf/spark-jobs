from datetime import timedelta

from airflow.hooks.base_hook import BaseHook

from config import email_addresses, slack_on_databricks_failure_callback
from custom_operators.databricks_functions import \
    DatabricksCreateClusterOperator, DatabricksTerminateClusterOperator, DatabricksSubmitRunOperator

ohub_country_codes = ['AD', 'AE', 'AF', 'AR', 'AT', 'AU', 'AZ', 'BD', 'BE', 'BG', 'BH', 'BO', 'BR', 'CA', 'CH',
                      'CL', 'CN', 'CO', 'CR', 'CZ', 'DE', 'DK', 'DO', 'EC', 'EE', 'EG', 'ES', 'FI', 'FR', 'GB',
                      'GE', 'GR', 'GT', 'HK', 'HN', 'HU', 'ID', 'IE', 'IL', 'IN', 'IR', 'IT', 'JO', 'KR', 'KW',
                      'LB', 'LK', 'LT', 'LU', 'LV', 'MA', 'MM', 'MO', 'MV', 'MX', 'MY', 'NI', 'NL', 'NO', 'NU',
                      'NZ', 'OM', 'PA', 'PE', 'PH', 'PK', 'PL', 'PT', 'QA', 'RO', 'RU', 'SA', 'SE', 'SG', 'SK',
                      'SV', 'TH', 'TR', 'TW', 'US', 'VE', 'VN', 'ZA']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': email_addresses,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'pool': 'ohub_pool',
    'on_failure_callback': slack_on_databricks_failure_callback
}


def default_cluster_config(cluster_name):
    return {
        "cluster_name": cluster_name,
        "spark_version": "4.0.x-scala2.11",
        "node_type_id": "Standard_DS5_v2",
        "autoscale": {
            "min_workers": '4',
            "max_workers": '12'
        },
        "autotermination_minutes": '30',
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
    }


databricks_conn_id = 'databricks_azure'


def create_cluster(task_id, cluster_config):
    return DatabricksCreateClusterOperator(
        task_id=task_id,
        databricks_conn_id=databricks_conn_id,
        cluster_config=cluster_config
    )


def terminate_cluster(task_id, cluster_name):
    return DatabricksTerminateClusterOperator(
        task_id=task_id,
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id
    )


postgres_connection = BaseHook.get_connection('postgres_channels')
postgres_config = [
    '--postgressUrl', postgres_connection.host,
    '--postgressUsername', postgres_connection.login,
    '--postgressPassword', postgres_connection.password,
    '--postgressDB', postgres_connection.schema
]


def ingest_task(schema, channel, clazz, cluster_name):
    return DatabricksSubmitRunOperator(
        task_id="{}_file_interface_to_parquet".format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': clazz,
            'parameters': ['--inputFile', raw_bucket.format(date=one_day_ago, schema=schema, channel=channel),
                           '--outputFile', ingested_bucket.format(date=one_day_ago, fn=schema, channel=channel),
                           '--strictIngestion', "false"] + postgres_config
        }
    )


def delta_fuzzy_matching_tasks(schema,
                               cluster_name,
                               delta_match_py,
                               match_py,
                               integrated_input,
                               ingested_input):
    tasks = {'in': [], 'out': []}
    for code in ohub_country_codes:
        match_new = DatabricksSubmitRunOperator(
            task_id=('match_new_{}_with_integrated_{}_{}'.format(schema, schema, code)),
            cluster_name=cluster_name,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'egg': egg}
            ],
            spark_python_task={
                'python_file': delta_match_py,
                'parameters': [
                    '--integrated_input_path', integrated_input,
                    '--ingested_daily_input_path', ingested_input,
                    '--updated_integrated_output_path',
                    intermediate_bucket.format(date=one_day_ago, fn='{}_fuzzy_matched_delta_integrated'.format(schema)),
                    '--unmatched_output_path',
                    intermediate_bucket.format(date=one_day_ago, fn='{}_delta_left_overs'.format(schema)),
                    '--country_code', code]
            }
        )

        match_unmatched = DatabricksSubmitRunOperator(
            task_id='match_unmatched_{}_{}'.format(schema, code),
            cluster_name=cluster_name,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'egg': egg}
            ],
            spark_python_task={
                'python_file': match_py,
                'parameters': ['--input_file',
                               intermediate_bucket.format(date=one_day_ago, fn='{}_delta_left_overs'.format(schema)),
                               '--output_path',
                               intermediate_bucket.format(date=one_day_ago, fn='{}_fuzzy_matched_delta'.format(schema)),
                               '--country_code', code]
            }
        )

        tasks['in'].append(match_new)
        tasks['out'].append(match_unmatched)
        match_new >> match_unmatched

    return tasks


interval = '@daily'
one_day_ago = '{{ds}}'
two_day_ago = '{{yesterday_ds}}'

wasb_conn_id = 'azure_blob'
container_name = 'prod'

jar = 'dbfs:/libraries/ohub/spark-jobs-assembly-0.2.0.jar'
egg = 'dbfs:/libraries/name_matching/string_matching.egg'

dbfs_root_bucket = 'dbfs:/mnt/ohub_data/'
raw_bucket = dbfs_root_bucket + 'raw/{schema}/{date}/{channel}/*.csv'
ingested_bucket = dbfs_root_bucket + 'ingested/{date}/{channel}/{fn}.parquet'
intermediate_bucket = dbfs_root_bucket + 'intermediate/{date}/{fn}.parquet'
integrated_bucket = dbfs_root_bucket + 'integrated/{date}/{fn}.parquet'
export_bucket = dbfs_root_bucket + 'export/{date}/{fn}'

wasb_root_bucket = 'data/'
wasb_raw_container = wasb_root_bucket + 'raw/{schema}/{date}/{channel}/*.csv'
wasb_ingested_container = wasb_root_bucket + 'ingested/{date}/{fn}.parquet'
wasb_intermediate_container = wasb_root_bucket + 'intermediate/{date}/{fn}.parquet'
wasb_integrated_container = wasb_root_bucket + 'integrated/{date}/{fn}.parquet'
wasb_export_container = wasb_root_bucket + 'export/{date}/{fn}'

http_root_bucket = 'https://{container}.blob.core.windows.net/{blob}/data/'
http_raw_container = http_root_bucket + 'raw/{schema}/{date}/{channel}/*.csv'
http_ingested_container = http_root_bucket + 'ingested/{date}/{fn}.parquet'
http_intermediate_container = http_root_bucket + 'intermediate/{date}/{fn}.parquet'
http_integrated_container = http_root_bucket + 'integrated/{date}/{fn}.parquet'
http_export_container = http_root_bucket + 'export/{date}/{fn}'
