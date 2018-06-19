import os
from datetime import timedelta
from typing import List

from airflow import AirflowException
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import ShortCircuitOperator

from config import email_addresses, slack_on_databricks_failure_callback
from custom_operators.databricks_functions import \
    DatabricksCreateClusterOperator, DatabricksTerminateClusterOperator, DatabricksSubmitRunOperator
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from custom_operators.file_from_wasb import FileFromWasbOperator
from custom_operators.empty_fallback import EmptyFallbackOperator

ohub_country_codes = ['AD', 'AE', 'AF', 'AR', 'AT', 'AU', 'AZ', 'BD', 'BE', 'BG', 'BH', 'BO', 'BR', 'CA', 'CH',
                      'CL', 'CN', 'CO', 'CR', 'CZ', 'DE', 'DK', 'DO', 'EC', 'EE', 'EG', 'ES', 'FI', 'FR', 'GB',
                      'GE', 'GR', 'GT', 'HK', 'HN', 'HU', 'ID', 'IE', 'IL', 'IN', 'IR', 'IT', 'JO', 'KR', 'KW',
                      'LB', 'LK', 'LT', 'LU', 'LV', 'MA', 'MM', 'MO', 'MV', 'MX', 'MY', 'NI', 'NL', 'NO', 'NU',
                      'NZ', 'OM', 'PA', 'PE', 'PH', 'PK', 'PL', 'PT', 'QA', 'RO', 'RU', 'SA', 'SE', 'SG', 'SK',
                      'SV', 'TH', 'TR', 'TW', 'US', 'VE', 'VN', 'ZA']


class DagConfig(object):
    def __init__(self, entity: str, is_delta: bool = False):
        self.entity = entity
        self.is_delta = is_delta
        self.schedule = '@daily' if is_delta else '@once'

    @property
    def dag_id(self):
        postfix = '_initial_load' if self._is_delta else ''
        return f'ohub_{self._entity}{postfix}'

    @property
    def cluster_name(self):
        return f'{self.dag_id}_{{{{ds}}}}'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': email_addresses,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack_on_databricks_failure_callback
}


def default_cluster_config(cluster_name):
    return {
        "cluster_name": cluster_name,
        "spark_version": "4.0.x-scala2.11",
        "node_type_id": "Standard_D16s_v3",
        "autoscale": {
            "min_workers": '4',
            "max_workers": '12'
        },
        "autotermination_minutes": '30',
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
    }


def small_cluster_config(cluster_name):
    return {
        "cluster_name": cluster_name,
        "spark_version": "4.0.x-scala2.11",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": '4',
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


class SubPipeline(object):
    def __init__(self, first_task: BaseOperator, last_task: BaseOperator):
        self.first_task = first_task
        self.last_task = last_task


class GenericPipeline(object):
    def __init__(self,
                 dag_config: DagConfig,
                 class_prefix: str):
        self._entity = dag_config.entity
        self._cluster_name = dag_config.cluster_name
        self._clazz = class_prefix

        self._is_delta = False

        self._exports: List[SubPipeline] = []
        self._ingests: List[SubPipeline] = []

    def has_ingest_from_file_interface(self,
                                       deduplicate_on_concat_id: bool = True,
                                       alternative_schema: str = None,
                                       alternative_output_fn: str = None,
                                       alternative_seperator: str = None) -> 'GenericPipeline':
        channel = 'file_interface'
        ingest_schema = alternative_schema if alternative_schema else self._entity
        input_file = raw_bucket.format(date=one_day_ago, schema=ingest_schema, channel=channel)
        output_file = alternative_output_fn if alternative_output_fn else ingested_bucket.format(date=one_day_ago,
                                                                                                 fn=self._entity,
                                                                                                 channel=channel)
        config = {
            'dedup': deduplicate_on_concat_id,
            'sep': alternative_seperator,
            'input': input_file,
            'output': output_file
        }
        self._ingests.append(self.__ingest_file_interface(config))
        return self

    def has_export_to_acm(self,
                          acm_schema_name: str,
                          extra_acm_parameters: List[str] = []) -> 'GenericPipeline':
        config = {
            'filename': 'acm/UFS_' + acm_schema_name + '_{{ds_nodash}}000000.csv',
            'extra_acm_parameters': extra_acm_parameters
        }
        self._exports.append(self.__export_acm_pipeline(config))
        return self

    def construct_ingest_pipeline(self) -> SubPipeline:
        cluster_up = create_cluster(self._entity, small_cluster_config(self._cluster_name))
        if self._is_delta:
            start_pipeline = ExternalTaskSensorOperator(
                task_id='start_pipeline',
                external_dag_id='TEST_DAG',
                external_task_id='All_Tasks_Completed',
                allowed_states=['success'],
                execution_delta=timedelta(minutes=30))
        else:
            start_pipeline = BashOperator(
                task_id='start_pipeline',
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
                file_path=wasb_raw_container.format(date=one_day_ago, schema=self._entity, channel='file_interface'),
                wasb_conn_id=wasb_conn_id)
            start_pipeline >> empty_fallback

        start_pipeline >> cluster_up

        t: SubPipeline
        for t in self._ingests:
            cluster_up >> t.first_task
            t.last_task >> end_ingest

        return SubPipeline(start_pipeline, end_ingest)

    def construct_export_pipeline(self) -> SubPipeline:
        cluster_down = terminate_cluster(self._entity, self._cluster_name)
        start_export = BashOperator(
            task_id='start_export',
            bash_command='echo "start export"',
        )

        end_pipeline = BashOperator(
            task_id='end_pipeline',
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
                                          match_py: str,
                                          integrated_input: str = None,
                                          delta_match_py: str = None, ) -> SubPipeline:

        if self._is_delta and (not integrated_input or not delta_match_py):
            raise AirflowException('cannot create delta matching without delta input params')
        if self._is_delta:
            return self.__delta_fuzzy_matching(delta_match_py=delta_match_py,
                                               integrated_input=integrated_input,
                                               match_py=match_py,
                                               ingested_input=ingest_input)
        else:
            return self.__fuzzy_matching(match_py=match_py, ingest_input=ingest_input)

    def __fuzzy_matching(self, match_py, ingest_input):
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
                task_id='match_{}_{}'.format(self._entity, country_code),
                cluster_name=self._cluster_name,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': match_py,
                    'parameters': ['--input_file', ingest_input,
                                   '--output_path',
                                   intermediate_bucket.format(date='{{ds}}', fn='{}_matched'.format(self._entity)),
                                   '--country_code', country_code,
                                   '--threshold', '0.9']
                }
            )
            start_matching >> t >> end_matching

        return SubPipeline(start_matching, end_matching)

    def __delta_fuzzy_matching(self,
                               delta_match_py,
                               match_py,
                               integrated_input,
                               ingested_input):
        start_matching = BashOperator(
            task_id='start_matching',
            bash_command='echo "start matching"',
        )
        end_matching = BashOperator(
            task_id='end_matching',
            bash_command='echo "end matching"',
        )

        for code in ohub_country_codes:
            match_new = DatabricksSubmitRunOperator(
                task_id=('match_new_{}_with_integrated_{}_{}'.format(self._entity, self._entity, code)),
                cluster_name=self._cluster_name,
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
                        intermediate_bucket.format(date=one_day_ago,
                                                   fn='{}_fuzzy_matched_delta_integrated'.format(self._entity)),
                        '--unmatched_output_path',
                        intermediate_bucket.format(date=one_day_ago, fn='{}_delta_left_overs'.format(self._entity)),
                        '--country_code', code]
                }
            )

            match_unmatched = DatabricksSubmitRunOperator(
                task_id='match_unmatched_{}_{}'.format(self._entity, code),
                cluster_name=self._cluster_name,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': match_py,
                    'parameters': ['--input_file',
                                   intermediate_bucket.format(date=one_day_ago,
                                                              fn='{}_delta_left_overs'.format(self._entity)),
                                   '--output_path',
                                   intermediate_bucket.format(date=one_day_ago,
                                                              fn='{}_fuzzy_matched_delta'.format(self._entity)),
                                   '--country_code', code]
                }
            )

            start_matching >> match_new >> match_unmatched >> end_matching

        return SubPipeline(start_matching, end_matching)

    def __ingest_file_interface(self, config: dict):
        sep = config['sep'] if config['sep'] else "\u2030"

        file_interface_to_parquet = ingest_task(
            schema=self._entity,
            channel='file_interface',
            clazz="com.unilever.ohub.spark.tsv2parquet.file_interface.{}Converter".format(self._clazz),
            field_separator=sep,
            cluster_name=self._cluster_name,
            deduplicate_on_concat_id=config['dedup'],
            ingest_input_schema=config['input'],
            ingest_output_file=config['output']
        )
        return SubPipeline(file_interface_to_parquet, file_interface_to_parquet)

    def __export_acm_pipeline(self, config: dict):
        previous_integrated = integrated_bucket.format(date=two_day_ago, fn=self._entity) if self._is_delta else None
        delta_params = ['--previousIntegrated', previous_integrated] if previous_integrated else []
        convert_to_acm = DatabricksSubmitRunOperator(
            task_id="{}_to_acm".format(self._entity),
            cluster_name=self._cluster_name,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'jar': jar}
            ],
            spark_jar_task={
                'main_class_name': "com.unilever.ohub.spark.acm.{}AcmConverter".format(self._clazz),
                'parameters': ['--inputFile', integrated_bucket.format(date='{{ds}}', fn=self._entity),
                               '--outputFile', export_bucket.format(date='{{ds}}', fn=config['filename'])] +
                              delta_params +
                              postgres_config +
                              config['extra_acm_parameters']
            }
        )

        tmp_file = '/tmp/' + config['filename']

        check_file_non_empty = ShortCircuitOperator(
            task_id='check_file_non_empty',
            python_callable=lambda: os.stat(tmp_file).st_size > 0
        )

        acm_from_wasb = FileFromWasbOperator(
            task_id='{}_acm_from_wasb'.format(self._entity),
            file_path=tmp_file,
            container_name=container_name,
            wasb_conn_id='azure_blob',
            blob_name=wasb_export_container.format(date='{{ds}}', fn=config['filename'])
        )

        ftp_to_acm = SFTPOperator(
            task_id='{}_ftp_to_acm'.format(self._entity),
            local_filepath=tmp_file,
            remote_filepath='/incoming/temp/ohub_2_test/{}'.format(tmp_file.split('/')[-1]),
            ssh_conn_id='acm_sftp_ssh',
            operation=SFTPOperation.PUT
        )
        convert_to_acm >> acm_from_wasb >> check_file_non_empty >> ftp_to_acm

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
