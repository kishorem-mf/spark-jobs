"""
Due to the highly modular nature of the design of the pipelines this file has been created to facilitate
writing DAGs using this modularity in mind.

Each DAG for the OHUB project will have a few partial pipelines in common, each will have:
start cluster -> ingest from various channels -> other processing -> export from various channels -> terminate cluster

Therefor the `GenericPipeline` class has been constructed. Using this one can construct partial pipelines (such as
ingest and export) with single lines of code and with minimal configuration.

All other `other processing` tasks must be constructed manually like any other airflow tasks must. Tying generated
partial pipelines with other tasks is made easy because each partial pipeline returns an object holding the
`first_task` and `last_task` of each respective pipeline.
"""

from datetime import timedelta
from typing import List

from airflow import AirflowException
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator

from config import email_addresses, slack_on_databricks_failure_callback
from custom_operators.databricks_functions import \
    DatabricksCreateClusterOperator, DatabricksTerminateClusterOperator, DatabricksSubmitRunOperator
from custom_operators.empty_fallback import EmptyFallbackOperator
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from custom_operators.file_from_wasb import FileFromWasbOperator

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
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack_on_databricks_failure_callback
}


def large_cluster_config(cluster_name: str):
    '''Returns a Databricks cluster configuration used for heavy tasks, such as string matching'''
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
    '''Returns a Databricks cluster configuration used for simple transformation tasks'''
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

postgres_connection = BaseHook.get_connection('postgres_channels')
postgres_config = [
    '--postgressUrl', postgres_connection.host,
    '--postgressUsername', postgres_connection.login,
    '--postgressPassword', postgres_connection.password,
    '--postgressDB', postgres_connection.schema
]


def create_cluster(entity: str, cluster_config: dict):
    '''Returns an Airflow tasks that tells Databricks to construct a cluster
    Args:
        entity: The entity (i.e. operators, contactpersons, etc.)
        cluster_config: A valid databricks cluster configuration
    '''
    return DatabricksCreateClusterOperator(
        task_id='{}_create_cluster'.format(entity),
        databricks_conn_id=databricks_conn_id,
        cluster_config=cluster_config
    )


def terminate_cluster(entity: str, cluster_name: str):
    '''Returns an Airflow tasks that tells Databricks to terminate a cluster

    Args:
        entity: The entity (i.e. operators, contactpersons, etc.)
        cluster_name: The cluster name
    '''
    return DatabricksTerminateClusterOperator(
        task_id='{}_terminate_cluster'.format(entity),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id
    )


class DagConfig(object):
    """This configuration holds the basic settings for each ohub DAG

    Args:
        entity: The entity (i.e. operators, contactpersons, etc.)
        is_delta: Tells the config if the DAG is initial load or if it is a delta process
        alternate_DAG_entity: Some entities might be part of the same DAG, this tells airflow to use that particular entity as main DAG id
        use_alternate_entity_as_cluster: Tells the generator to use the given alternate DAG id as cluster name, usually you don't want this

    """

    def __init__(self,
                 entity: str,
                 is_delta: bool,
                 alternate_DAG_entity: str = None,
                 use_alternate_entity_as_cluster: bool = False):
        self.entity = entity
        self.is_delta = is_delta
        self.use_alternate_entity_as_cluster = use_alternate_entity_as_cluster
        self.alternate_DAG_entity = alternate_DAG_entity
        self.schedule = '@daily' if is_delta else '@once'

    @property
    def dag_id(self):
        postfix = '_initial_load' if not self.is_delta else ''
        entity = self.alternate_DAG_entity if self.alternate_DAG_entity else self.entity
        return f'ohub_{entity}{postfix}'

    @property
    def cluster_name(self):
        if self.use_alternate_entity_as_cluster and self.alternate_DAG_entity:
            return f'{self.dag_id}_{{{{ds}}}}'
        else:
            postfix = '_initial_load' if not self.is_delta else ''
            return f'ohub_{self.entity}{postfix}_{{{{ds}}}}'


class IngestConfig(object):
    """Configuration class for ingest tasks

    Args:
         input_file: The file or directory to read the input from
         output_file: Where to put the output parquet
         channel: The channel of the raw files (i.e. `file_interface`, `web_event`, etc)
         deduplicate_on_concat_id: Whether to deduplicate on `concatId`, in most cases this should be `True`
         alternative_seperator: The seperator for the CSV files
    """
    def __init__(self,
                 input_file: str,
                 output_file: str,
                 channel: str,
                 deduplicate_on_concat_id: bool = True,
                 alternative_seperator: str = None):
        self.dedup = deduplicate_on_concat_id
        self.sep = alternative_seperator
        self.input = input_file
        self.output = output_file
        self.channel = channel


class SubPipeline(object):
    """A (case) class holding the first and last task of a pipeline"""
    def __init__(self, first_task: BaseOperator, last_task: BaseOperator):
        self.first_task = first_task
        self.last_task = last_task


class GenericPipeline(object):
    """Generates commonly used partial pipelines. Supported partial pipelines are:

    * ingest, includes cluster creation, from:

      - File interface
      - Web event
      - Emakina (not implemented yet)
      - Sifu (not implemented yet)

    * export, includes cluster termination, to

      - ACM
      - DispacherDB

    * fuzzy string matching for initial load and delta

    Args:
        dag_config: The DAG configuration
        class_prefix: The spark job class prefix (i.e `Operator`, `Product` etc)
        cluster_config: The configuration for the cluster
    """
    def __init__(self,
                 dag_config: DagConfig,
                 class_prefix: str,
                 cluster_config: dict):
        self._clazz = class_prefix
        self._dag_config = dag_config
        self._cluster_config = cluster_config

        self._exports: List[SubPipeline] = []
        self._ingests: List[SubPipeline] = []

    def has_ingest_from_file_interface(self,
                                       deduplicate_on_concat_id: bool = True,
                                       alternative_schema: str = None,
                                       alternative_output_fn: str = None,
                                       alternative_seperator: str = None) -> 'GenericPipeline':
        '''Marks the pipeline to include ingest from file interface'''
        channel = 'file_interface'
        ingest_schema = alternative_schema if alternative_schema else self._dag_config.entity
        input_file = raw_bucket.format(date=one_day_ago, schema=ingest_schema, channel=channel)
        output_file = alternative_output_fn if alternative_output_fn else ingested_bucket.format(date=one_day_ago,
                                                                                                 fn=self._dag_config.entity,
                                                                                                 channel=channel)
        config = IngestConfig(
            input_file=input_file,
            output_file=output_file,
            channel=channel,
            deduplicate_on_concat_id=deduplicate_on_concat_id,
            alternative_seperator=alternative_seperator)

        self._ingests.append(self.__ingest_from_channel(config))
        return self

    def has_ingest_from_web_event(self):
        '''Marks the pipeline to include ingest from web event'''
        # channel = 'web_event'
        # config = {}
        # self._ingests.append(self.__ingest_web_event(config))
        return self

    def has_export_to_acm(self,
                          acm_schema_name: str,
                          extra_acm_parameters: List[str] = []) -> 'GenericPipeline':
        '''Marks the pipeline to include export to ACM'''
        config = {
            'filename': 'acm/UFS_' + acm_schema_name + '_{{ds_nodash}}000000.csv',
            'extra_acm_parameters': extra_acm_parameters
        }
        self._exports.append(self.__export_acm_pipeline(config))
        return self

    def has_export_to_dispach(self):
        '''Marks the pipeline to include export to dispatcherDb'''
        # config = {}
        # self._exports.append(self.__export_dispach_pipeline(config))
        return self

    def construct_ingest_pipeline(self) -> SubPipeline:
        """
        Constructs the full ingest pipeline.

        It will loop over `self._ingests` and add the tasks as parallel to the DAG.
        Depending on if the Pipeline is delta or not it will create a `SensorOperator` that waits for the previous day
        to be completed.

        This will also boot up a cluster
        """
        cluster_up = create_cluster(self._dag_config.entity, self._cluster_config)
        if self._dag_config.is_delta:
            start_pipeline = ExternalTaskSensorOperator(
                task_id=f'{self._dag_config.entity}_start_pipeline',
                external_dag_id=self._dag_config.dag_id,
                external_task_id=f'{self._dag_config.entity}_end_pipeline',
                execution_delta=timedelta(days=1))
        else:
            start_pipeline = BashOperator(
                task_id=f'{self._dag_config.entity}_start_pipeline',
                bash_command='echo "start pipeline"',
            )

        end_ingest = BashOperator(
            task_id=f'{self._dag_config.entity}_end_ingest',
            bash_command='echo "end ingesting"',
        )

        if self._dag_config.is_delta:
            empty_fallback = EmptyFallbackOperator(
                task_id=f'{self._dag_config.entity}_empty_fallback',
                container_name='prod',
                file_path=wasb_raw_container.format(date=one_day_ago, schema=self._dag_config.entity,
                                                    channel='file_interface'),
                wasb_conn_id=wasb_conn_id)
            start_pipeline >> empty_fallback

        gather = DatabricksSubmitRunOperator(
            task_id=f"{self._dag_config.entity}_gather",
            cluster_name=self._dag_config.cluster_name,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'jar': jar}
            ],
            spark_jar_task={
                'main_class_name': f'com.unilever.ohub.spark.tsv2parquet.GatherJob',
                'parameters': ['--input',
                               ingested_bucket.format(date=one_day_ago, channel='*', fn=self._dag_config.entity),
                               '--output',
                               intermediate_bucket.format(date=one_day_ago, fn=f'{self._dag_config.entity}_gathered')]
            }
        )
        start_pipeline >> cluster_up
        gather >> end_ingest

        t: SubPipeline
        for t in self._ingests:
            cluster_up >> t.first_task
            t.last_task >> gather

        return SubPipeline(start_pipeline, end_ingest)

    def construct_export_pipeline(self) -> SubPipeline:
        """
        Constructs the full export pipeline.

        It will loop over `self._exports` and add the tasks as parallel to the DAG.

        This will also terminate the cluster
        """
        cluster_down = terminate_cluster(self._dag_config.entity, self._dag_config.cluster_name)
        start_export = BashOperator(
            task_id=f'{self._dag_config.entity}_start_export',
            bash_command='echo "start export"',
        )

        end_pipeline = BashOperator(
            task_id=f'{self._dag_config.entity}_end_pipeline',
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
        ''' Constructs the fuzzy string matching pipeline, adding tasks PER country.
        Depending on whether the pipeline `is_delta` is returns the delta or initial load matching partial pipeline
        '''

        if self._dag_config.is_delta and (not integrated_input or not delta_match_py):
            raise AirflowException('cannot create delta matching without delta input params')
        if self._dag_config.is_delta:
            return self.__delta_fuzzy_matching(delta_match_py=delta_match_py,
                                               integrated_input=integrated_input,
                                               match_py=match_py,
                                               ingested_input=ingest_input)
        else:
            return self.__fuzzy_matching(match_py=match_py, ingest_input=ingest_input)

    def __fuzzy_matching(self, match_py, ingest_input):
        start_matching = BashOperator(
            task_id=f'{self._dag_config.entity}_start_matching',
            bash_command='echo "start matching"',
        )
        end_matching = BashOperator(
            task_id=f'{self._dag_config.entity}_end_matching',
            bash_command='echo "end matching"',
        )
        for country_code in ohub_country_codes:
            t = DatabricksSubmitRunOperator(
                task_id='match_{}_{}'.format(self._dag_config.entity, country_code),
                cluster_name=self._dag_config.cluster_name,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': match_py,
                    'parameters': ['--input_file', ingest_input,
                                   '--output_path',
                                   intermediate_bucket.format(date='{{ds}}',
                                                              fn='{}_matched'.format(self._dag_config.entity)),
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
                task_id=('match_new_{}_with_integrated_{}_{}'.format(self._dag_config.entity, self._dag_config.entity,
                                                                     code)),
                cluster_name=self._dag_config.cluster_name,
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
                                                   fn='{}_fuzzy_matched_delta_integrated'.format(
                                                       self._dag_config.entity)),
                        '--unmatched_output_path',
                        intermediate_bucket.format(date=one_day_ago,
                                                   fn='{}_delta_left_overs'.format(self._dag_config.entity)),
                        '--country_code', code]
                }
            )

            match_unmatched = DatabricksSubmitRunOperator(
                task_id='match_unmatched_{}_{}'.format(self._dag_config.entity, code),
                cluster_name=self._dag_config.cluster_name,
                databricks_conn_id=databricks_conn_id,
                libraries=[
                    {'egg': egg}
                ],
                spark_python_task={
                    'python_file': match_py,
                    'parameters': ['--input_file',
                                   intermediate_bucket.format(date=one_day_ago,
                                                              fn='{}_delta_left_overs'.format(self._dag_config.entity)),
                                   '--output_path',
                                   intermediate_bucket.format(date=one_day_ago,
                                                              fn='{}_fuzzy_matched_delta'.format(
                                                                  self._dag_config.entity)),
                                   '--country_code', code]
                }
            )

            start_matching >> match_new >> match_unmatched >> end_matching

        return SubPipeline(start_matching, end_matching)

    def __ingest_from_channel(self, config: IngestConfig):
        sep = config.sep if config.sep else "\u2030"
        dedup = 'true' if config.dedup else 'false'

        ingest_to_parquet = DatabricksSubmitRunOperator(
            task_id=f"{self._dag_config.entity}_{config.channel}_to_parquet",
            cluster_name=self._dag_config.cluster_name,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'jar': jar}
            ],
            spark_jar_task={
                'main_class_name': f'com.unilever.ohub.spark.tsv2parquet.{config.channel}.{self._clazz}Converter',
                'parameters': ['--inputFile', config.input,
                               '--outputFile', config.output,
                               '--fieldSeparator', sep,
                               '--strictIngestion', "false",
                               '--deduplicateOnConcatId', dedup] + postgres_config
            },
        )

        return SubPipeline(ingest_to_parquet, ingest_to_parquet)

    def __export_acm_pipeline(self, config: dict):
        previous_integrated = integrated_bucket.format(date=two_day_ago,
                                                       fn=self._dag_config.entity) if self._dag_config.is_delta else None
        delta_params = ['--previousIntegrated', previous_integrated] if previous_integrated else []
        convert_to_acm = DatabricksSubmitRunOperator(
            task_id=f"{self._dag_config.entity}_to_acm",
            cluster_name=self._dag_config.cluster_name,
            databricks_conn_id=databricks_conn_id,
            libraries=[
                {'jar': jar}
            ],
            spark_jar_task={
                'main_class_name': "com.unilever.ohub.spark.acm.{}AcmConverter".format(self._clazz),
                'parameters': ['--inputFile', integrated_bucket.format(date='{{ds}}', fn=self._dag_config.entity),
                               '--outputFile', export_bucket.format(date='{{ds}}', fn=config['filename'])] +
                              delta_params +
                              postgres_config +
                              config['extra_acm_parameters']
            }
        )

        tmp_file = '/tmp/' + config['filename']

        # check_file_non_empty = CheckFileNonEmptyOperator(
        #     task_id=f'{self._dag_config.entity}_check_file_non_empty',
        #     file_path=tmp_file
        # )

        acm_from_wasb = FileFromWasbOperator(
            task_id=f'{self._dag_config.entity}_acm_from_wasb',
            file_path=tmp_file,
            container_name=container_name,
            wasb_conn_id='azure_blob',
            blob_name=wasb_export_container.format(date='{{ds}}', fn=config['filename'])
        )

        ftp_to_acm = SFTPOperator(
            task_id=f'{self._dag_config.entity}_ftp_to_acm',
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

http_root_bucket = 'https://{container}.blob.core.windows.net/data/data/'
http_raw_container = http_root_bucket + 'raw/{schema}/{date}/{channel}/*.csv'
http_ingested_container = http_root_bucket + 'ingested/{date}/{fn}.parquet'
http_intermediate_container = http_root_bucket + 'intermediate/{date}/{fn}.parquet'
http_integrated_container = http_root_bucket + 'integrated/{date}/{fn}.parquet'
http_export_container = http_root_bucket + 'export/{date}/{fn}'
