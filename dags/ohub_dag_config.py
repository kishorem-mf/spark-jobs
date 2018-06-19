from datetime import timedelta
from typing import List

from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator

from config import email_addresses, slack_on_databricks_failure_callback
from custom_operators.databricks_functions import \
    DatabricksCreateClusterOperator, DatabricksTerminateClusterOperator, DatabricksSubmitRunOperator
from custom_operators.file_from_wasb import FileFromWasbOperator
from custom_operators.empty_fallback import EmptyFallbackOperator

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
        "autotermination_minutes": '10',
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
    }


def small_cluster_config(cluster_name):
    return {
        "cluster_name": cluster_name,
        "spark_version": "4.0.x-scala2.11",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": '2',
        "autotermination_minutes": '60',
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
    }


databricks_conn_id = 'databricks_azure'


def create_cluster(schema, cluster_config):
    return DatabricksCreateClusterOperator(
        task_id='{}_create_cluster'.format(schema),
        databricks_conn_id=databricks_conn_id,
        cluster_config=cluster_config
    )


def terminate_cluster(schema, cluster_name):
    return DatabricksTerminateClusterOperator(
        task_id='{}_terminate_cluster'.format(schema),
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


def ingest_task(schema, channel, clazz,
                cluster_name,
                field_separator=';',
                deduplicate_on_concat_id=True,
                ingest_input_schema=None,
                ingest_output_file=None):
    dedup = 'true' if deduplicate_on_concat_id else 'false'

    ingest_schema = ingest_input_schema if ingest_input_schema else schema
    input_file = raw_bucket.format(date=one_day_ago, schema=ingest_schema, channel=channel)
    output_file = ingest_output_file if ingest_output_file else ingested_bucket.format(date=one_day_ago, fn=schema,
                                                                                       channel=channel)

    return DatabricksSubmitRunOperator(
        task_id="{}_file_interface_to_parquet".format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': clazz,
            'parameters': ['--inputFile', input_file,
                           '--outputFile', output_file,
                           '--fieldSeparator', field_separator,
                           '--strictIngestion', "false",
                           '--deduplicateOnConcatId', dedup] + postgres_config
        },
        depends_on_past=True
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


class SubPipeline(object):
    def __init__(self, first_task: BaseOperator, last_task: BaseOperator):
        self.first_task = first_task
        self.last_task = last_task


class GenericPipeline(object):
    def __init__(self,
                 schema: str,
                 cluster_name: str,
                 clazz: str
                 ):
        self._schema = schema
        self._cluster_name = cluster_name
        self._clazz = clazz

        self._is_delta = False

        self._exports: List(SubPipeline) = []
        self._ingests: List(SubPipeline) = []

    def is_delta(self):
        self._is_delta = True
        return self

    def has_ingest_from_file_interface(self,
                                       deduplicate_on_concat_id: bool = True,
                                       alternative_schema: str = None,
                                       alternative_output_fn: str = None) -> 'GenericPipeline':
        channel = 'file_interface'
        ingest_schema = alternative_schema if alternative_schema else self._schema
        input_file = raw_bucket.format(date=one_day_ago, schema=ingest_schema, channel=channel)
        output_file = alternative_output_fn if alternative_output_fn else ingested_bucket.format(date=one_day_ago,
                                                                                                 fn=self._schema,
                                                                                                 channel=channel)
        config = {
            'dedup': deduplicate_on_concat_id,
            'input': input_file,
            'output': output_file
        }
        self._ingests.append(self.__ingest_file_interface(config))
        return self

    def has_export_to_acm(self,
                          acm_schema_name: str,
                          extra_acm_parameters: List(str) = []) -> 'GenericPipeline':
        config = {
            'filename': 'acm/UFS_' + acm_schema_name + '_{{ds_nodash}}000000.csv',
            'extra_acm_parameters': extra_acm_parameters
        }
        self._exports.append(self.__export_acm_pipeline(config))
        return self

    def construct_ingest_pipeline(self) -> SubPipeline:
        cluster_up = create_cluster(self._schema, small_cluster_config(self._cluster_name))
        start_pipeline = BashOperator(
            task_id='start_{}'.format(self._schema),
            bash_command='echo "start pipeline"',
        )

        end_ingest = BashOperator(
            task_id='end_ingest',
            bash_command='echo "end ingesting"',
        )

        if self._is_delta:
            empty_fallback = EmptyFallbackOperator(
                task_id='empty_fallback',
                container_name='prod',
                file_path=wasb_raw_container.format(date=one_day_ago, schema=self._schema, channel='file_interface'),
                wasb_conn_id=wasb_conn_id)
            start_pipeline >> empty_fallback

        start_pipeline >> cluster_up

        t: SubPipeline
        for t in self._ingests:
            cluster_up >> t.first_task
            t.last_task >> end_ingest

        return SubPipeline(start_pipeline, end_ingest)

    def construct_export_pipeline(self) -> SubPipeline:
        cluster_down = terminate_cluster(self._schema, self._cluster_name)
        start_export = BashOperator(
            task_id='start_export'.format(self._schema),
            bash_command='echo "start export"',
        )

        end_pipeline = BashOperator(
            task_id='end_ingest',
            bash_command='echo "end pipeline"',
        )

        cluster_down >> end_pipeline

        t: SubPipeline
        for t in self._exports:
            start_export >> t.first_task
            t.last_task >> cluster_down
        return SubPipeline(start_export, end_pipeline)

    def construct_fuzzy_matching_pipeline(self,
                                 ingest_input: str,
                                 match_py: str) -> SubPipeline:
        start_matching = BashOperator(
            task_id='start_matching',
            bash_command='echo "start matching"',
        )
        end_matching = BashOperator(
            task_id='end_matching',
            bash_command='echo "end matching"',
        )
        for country_code in ohub_country_codes:
            t = DatabricksSubmitRunOperator(
                task_id='match_{}_{}'.format(self._schema, country_code),
                cluster_name=self._cluster_name,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': match_py,
                    'parameters': ['--input_file', ingest_input,
                                   '--output_path',
                                   intermediate_bucket.format(date='{{ds}}', fn='{}_matched'.format(self._schema)),
                                   '--country_code', country_code,
                                   '--threshold', '0.9']
                }
            )
            start_matching >> t >> end_matching

        return SubPipeline(start_matching, end_matching)

    def __ingest_file_interface(self, config: dict):
        file_interface_to_parquet = ingest_task(
            schema=self._schema,
            channel='file_interface',
            clazz="com.unilever.ohub.spark.tsv2parquet.file_interface.{}Converter".format(self._clazz),
            field_separator=u"\u2030",
            cluster_name=self._cluster_name,
            deduplicate_on_concat_id=config['dedup'],
            ingest_input_schema=config['input'],
            ingest_output_file=config['output']
        )
        return SubPipeline(file_interface_to_parquet, file_interface_to_parquet)

    def __export_acm_pipeline(self, config: dict):
        previous_integrated = integrated_bucket.format(date=two_day_ago, fn=self._schema) if self._is_delta else None
        delta_params = ['--previousIntegrated', previous_integrated] if previous_integrated else []
        convert_to_acm = DatabricksSubmitRunOperator(
            task_id="{}_to_acm".format(self._schema),
            cluster_name=self._cluster_name,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'jar': jar}
            ],
            spark_jar_task={
                'main_class_name': "com.unilever.ohub.spark.acm.{}AcmConverter".format(self._clazz),
                'parameters': ['--inputFile', integrated_bucket.format(date='{{ds}}', fn=self._schema),
                               '--outputFile', export_bucket.format(date='{{ds}}', fn=config['filename'])] +
                              delta_params +
                              postgres_config +
                              config['extra_acm_parameters']
            }
        )

        tmp_file = '/tmp/' + config['filename']

        acm_from_wasb = FileFromWasbOperator(
            task_id='{}_acm_from_wasb'.format(self._schema),
            file_path=tmp_file,
            container_name=container_name,
            wasb_conn_id='azure_blob',
            blob_name=wasb_export_container.format(date='{{ds}}', fn=config['filename'])
        )

        ftp_to_acm = SFTPOperator(
            task_id='{}_ftp_to_acm'.format(self._schema),
            local_filepath=tmp_file,
            remote_filepath='/incoming/temp/ohub_2_test/{}'.format(tmp_file.split('/')[-1]),
            ssh_conn_id='acm_sftp_ssh',
            operation=SFTPOperation.PUT
        )
        convert_to_acm >> acm_from_wasb >> ftp_to_acm

        return SubPipeline(convert_to_acm, ftp_to_acm)


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
