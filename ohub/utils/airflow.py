from typing import List

from airflow import configuration, AirflowException
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from ohub.operators.databricks_operator import (
    DatabricksCreateClusterOperator,
    DatabricksSubmitRunOperator,
    DatabricksTerminateClusterOperator,
)
from ohub.operators.wasb_operator import EmptyFallbackOperator
from ohub.operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub.operators.wasb_operator import FileFromWasbOperator


class LazyConnection(object):
    """Lazy connection class that only fetches connection when accessed.

    :param str _conn_id: Airflow connection id.
    """

    def __init__(self, conn_id: str):
        self._conn = None
        self._conn_id = conn_id

    def __getattr__(self, name):
        if self._conn is None:
            self._conn = BaseHook.get_connection(conn_id=self._conn_id)
        return getattr(self._conn, name)


class DagConfig(object):
    """This configuration holds the basic settings for each OHUB DAG

    Args:
        entity: The entity (i.e. operators, contactpersons, etc.)
        is_delta: Tells the config if the DAG is initial load or if it is a delta process
        alternate_DAG_entity: Some entities might be part of the same DAG, this tells airflow to use that particular entity as main DAG id
        use_alternate_entity_as_cluster: Tells the generator to use the given alternate DAG id as cluster name, usually you don't want this

    """

    def __init__(
        self,
        entity: str,
        is_delta: bool,
        alternate_dag_entity: str = None,
        use_alternate_entity_as_cluster: bool = False,
    ):
        self.entity = entity
        self.is_delta = is_delta
        self._use_alternate_entity_as_cluster = use_alternate_entity_as_cluster
        self._alternate_dag_entity = alternate_dag_entity
        self.schedule = "@daily" if is_delta else "@once"

    @property
    def dag_id(self):
        postfix = "_initial_load" if not self.is_delta else ""
        entity = self._alternate_dag_entity if self._alternate_dag_entity else self.entity
        return f"ohub_{entity}{postfix}"

    @property
    def cluster_name(self):
        if self._use_alternate_entity_as_cluster and self._alternate_dag_entity:
            return f"{self.dag_id}_{{{{ ds }}}}"
        else:
            postfix = "_initial_load" if not self.is_delta else ""
            return f"ohub_{self.entity}{postfix}_{{{{ ds }}}}"


class SubPipeline(object):
    """A class holding the first and last task of a pipeline"""

    def __init__(self, first_task: BaseOperator, last_task: BaseOperator):
        self.first_task = first_task
        self.last_task = last_task


class IngestConfig(object):
    """Configuration class for ingest tasks

    Args:
         input_file: The file or directory to read the input from
         output_file: Where to put the output parquet
         channel: The channel of the raw files (i.e. `file_interface`, `web_event`, etc)
         deduplicate_on_concat_id: Whether to deduplicate on `concatId`, in most cases this should be `True`
         alternative_seperator: The separator for the CSV files
    """

    def __init__(
        self,
        input_file: str,
        output_file: str,
        channel: str,
        deduplicate_on_concat_id: bool = True,
        separator: str = None,
    ):
        self.dedup = deduplicate_on_concat_id
        self.separator = separator
        self.input = input_file
        self.output = output_file
        self.channel = channel


class GenericPipeline(object):
    """
    Due to the highly modular nature of the design of the pipelines this file has been created to facilitate
    writing DAGs using this modularity in mind.

    Each DAG for the OHUB project will have a few partial pipelines in common, each will have:
    start cluster -> ingest from various channels -> other processing -> export from various channels -> terminate cluster

    Therefore the `GenericPipeline` class has been constructed. Using this one can construct partial pipelines (such as
    ingest and export) with single lines of code and with minimal configuration.

    All other `other processing` tasks must be constructed manually like any other airflow tasks must. Tying generated
    partial pipelines with other tasks is made easy because each partial pipeline returns an object holding the
    `first_task` and `last_task` of each respective pipeline.

    Generates commonly used partial pipelines. Supported partial pipelines are:

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

    def __init__(
        self,
        dag_config: DagConfig,
        databricks_conn_id: str,
        class_prefix: str,
        cluster_config: dict,
        spark_jobs_jar: str,
        wasb_raw_container: str,
        wasb_conn_id: str,
        ingested_bucket: str,
        intermediate_bucket: str,
    ):
        self._clazz = class_prefix
        self._dag_config = dag_config
        self._databricks_conn_id = databricks_conn_id
        self._cluster_config = cluster_config
        self._spark_jobs_jar = spark_jobs_jar
        self._wasb_raw_container = wasb_raw_container
        self._wasb_conn_id = wasb_conn_id
        self._ingested_bucket = ingested_bucket
        self._intermediate_bucket = intermediate_bucket

        self._exports: List[SubPipeline] = []
        self._ingests: List[SubPipeline] = []

    def has_ingest_from_file_interface(
        self,
        raw_bucket: str,
        deduplicate_on_concat_id: bool = True,
        alternative_schema: str = None,
    ) -> "GenericPipeline":
        """Marks the pipeline to include ingest from file interface"""
        channel = "file_interface"
        ingest_schema = (
            alternative_schema if alternative_schema else self._dag_config.entity
        )
        input_file = raw_bucket.format(
            date="{{ ds }}", schema=ingest_schema, channel=channel
        )
        output_file = self._ingested_bucket.format(
            date="{{ ds }}", fn=self._dag_config.entity, channel=channel
        )

        separator = ";"

        config = IngestConfig(
            input_file=input_file,
            output_file=output_file,
            channel=channel,
            deduplicate_on_concat_id=deduplicate_on_concat_id,
            separator=separator,
        )

        self._ingests.append(self.__ingest_from_channel(config))
        return self

    def has_ingest_from_web_event(
        self,
        raw_bucket: str,
        ingested_bucket: str,
        deduplicate_on_concat_id: bool = True,
        alternative_schema: str = None,
        alternative_output_fn: str = None,
    ) -> "GenericPipeline":
        """Marks the pipeline to include ingest from web event"""
        channel = "web_event_interface"
        ingest_schema = (
            alternative_schema if alternative_schema else self._dag_config.entity
        )
        input_file = raw_bucket.format(
            date="{{ ds }}", schema=ingest_schema, channel=channel
        )
        output_file = (
            alternative_output_fn
            if alternative_output_fn
            else ingested_bucket.format(
                date="{{ ds }}", fn=self._dag_config.entity, channel=channel
            )
        )
        separator = ";"
        config = IngestConfig(
            input_file=input_file,
            output_file=output_file,
            channel=channel,
            deduplicate_on_concat_id=deduplicate_on_concat_id,
            separator=separator,
        )

        # self._ingests.append(self.__ingest_from_channel(config))

        return self

    def has_export_to_acm(
        self,
        acm_schema_name: str,
        integrated_bucket: str,
        export_bucket: str,
        container_name: str,
        wasb_export_container: str,
        extra_acm_parameters: List[str] = None,
    ) -> "GenericPipeline":
        """Marks the pipeline to include export to ACM"""
        if not extra_acm_parameters:
            extra_acm_parameters = []

        self._exports.append(
            self.__export_acm_pipeline(
                integrated_bucket=integrated_bucket,
                filename=f"acm/UFS_{acm_schema_name}_{{ ds_nodash }}000000.csv",
                export_bucket=export_bucket,
                container_name=container_name,
                wasb_export_container=wasb_export_container,
                extra_acm_parameters=extra_acm_parameters,
            )
        )
        return self

    def has_export_to_dispatcher_db(
        self,
        dispatcher_schema_name: str,
        integrated_bucket: str,
        export_bucket: str,
    ):
        """Marks the pipeline to include export to dispatcherDb"""
        filename = (
            "dispatcherdb/UFS_DISPATCHER_"
            + dispatcher_schema_name
            + "_{{ds_nodash}}000000.csv"
        )
        self._exports.append(
            self.__export_dispatch_pipeline(
                filename=filename,
                integrated_bucket=integrated_bucket,
                export_bucket=export_bucket,
            )
        )
        return self

    def __create_cluster(
        self, entity: str, cluster_config: dict, databricks_conn_id: str
    ):
        """Returns an Airflow tasks that tells Databricks to construct a cluster
        Args:
            entity: The entity (i.e. operators, contactpersons, etc.)
            cluster_config: A valid databricks cluster configuration
        """
        return DatabricksCreateClusterOperator(
            task_id="{}_create_cluster".format(entity),
            databricks_conn_id=databricks_conn_id,
            cluster_config=cluster_config,
        )

    def __terminate_cluster(
        self, entity: str, cluster_name: str, databricks_conn_id: str
    ):
        """Returns an Airflow tasks that tells Databricks to terminate a cluster

        Args:
            entity: The entity (i.e. operators, contactpersons, etc.)
            cluster_name: The cluster name
        """
        return DatabricksTerminateClusterOperator(
            task_id="{}_terminate_cluster".format(entity),
            cluster_name=cluster_name,
            databricks_conn_id=databricks_conn_id,
            trigger_rule=TriggerRule.ALL_DONE,
        )

    def construct_ingest_pipeline(self) -> SubPipeline:
        """
        Constructs the full ingest pipeline.

        It will loop over `self._ingests` and add the tasks as parallel to the DAG.
        Depending on if the Pipeline is delta or not it will create a `SensorOperator` that waits for the previous day
        to be completed.

        This will also boot up a cluster
        """
        entity = self._dag_config.entity
        cluster_up = self.__create_cluster(
            entity=entity,
            cluster_config=self._cluster_config,
            databricks_conn_id=self._databricks_conn_id,
        )

        start_pipeline = BashOperator(
            task_id=f"{entity}_start_pipeline",
            bash_command='echo "start pipeline"',
        )

        start_ingest = BashOperator(
            task_id=f"{entity}_start_ingest",
            bash_command='echo "start ingest"',
        )

        end_ingest = BashOperator(
            task_id=f"{entity}_end_ingest",
            bash_command='echo "end ingesting"',
        )

        gather = DatabricksSubmitRunOperator(
            task_id=f"{self._dag_config.entity}_gather",
            cluster_name=self._dag_config.cluster_name,
            databricks_conn_id=self._databricks_conn_id,
            libraries=[{"jar": self._spark_jobs_jar}],
            spark_jar_task={
                "main_class_name": f"com.unilever.ohub.spark.ingest.GatherJob",
                "parameters": [
                    "--input",
                    self._ingested_bucket.format(
                        date="{{ ds }}", channel="*", fn=self._dag_config.entity
                    ),
                    "--output",
                    self._intermediate_bucket.format(
                        date="{{ ds }}", fn=f"{self._dag_config.entity}_gathered"
                    ),
                ],
            },
        )

        if self._dag_config.is_delta:
            first_ingest_sensor = ExternalTaskSensorOperator(
                task_id=f'{entity}_first_ingest_sensor',
                external_dag_id=f'ohub_{entity}_initial_load',
                external_task_id=f'{entity}_end_pipeline'
            )

            empty_fallback = EmptyFallbackOperator(
                task_id=f"{entity}_empty_fallback",
                container_name="prod",
                file_path=self._wasb_raw_container.format(
                    date="{{ ds }}",
                    schema=entity,
                    channel="file_interface",
                ),
                wasb_conn_id=self._wasb_conn_id,
            )
            start_pipeline >> first_ingest_sensor >> empty_fallback >> start_ingest
        else:
            start_pipeline >> start_ingest

        start_ingest >> cluster_up
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
        cluster_down = self.__terminate_cluster(
            self._dag_config.entity,
            self._dag_config.cluster_name,
            databricks_conn_id=self._databricks_conn_id,
        )

        start_export = BashOperator(
            task_id=f"{self._dag_config.entity}_start_export",
            bash_command='echo "start export"',
        )

        end_pipeline = BashOperator(
            task_id=f"{self._dag_config.entity}_end_pipeline",
            bash_command='echo "end pipeline"',
        )

        cluster_down >> end_pipeline

        t: SubPipeline
        for t in self._exports:
            start_export >> t.first_task
            t.last_task >> cluster_down
        return SubPipeline(start_export, end_pipeline)

    def construct_fuzzy_matching_pipeline(
        self,
        ingest_input: str,
        match_py: str,
        ohub_country_codes: List[str],
        string_matching_egg: str,
        integrated_input: str = None,
        delta_match_py: str = None,
    ) -> SubPipeline:
        """ Constructs the fuzzy string matching pipeline, adding tasks PER country.
        Depending on whether the pipeline `is_delta` is returns the delta or initial load matching partial pipeline
        """

        if self._dag_config.is_delta and (not integrated_input or not delta_match_py):
            raise AirflowException(
                "cannot create delta matching without delta input params"
            )
        if self._dag_config.is_delta:
            return self.__delta_fuzzy_matching(
                delta_match_py=delta_match_py,
                integrated_input=integrated_input,
                match_py=match_py,
                ingested_input=ingest_input,
                ohub_country_codes=ohub_country_codes,
                string_matching_egg=string_matching_egg,
            )
        else:
            return self.__fuzzy_matching(
                match_py=match_py,
                ingest_input=ingest_input,
                ohub_country_codes=ohub_country_codes,
                string_matching_egg=string_matching_egg,
            )

    def __fuzzy_matching(
        self,
        match_py,
        ingest_input,
        ohub_country_codes: List[str],
        string_matching_egg: str,
    ):
        start_matching = BashOperator(
            task_id=f"{self._dag_config.entity}_start_matching",
            bash_command='echo "start matching"',
        )
        end_matching = BashOperator(
            task_id=f"{self._dag_config.entity}_end_matching",
            bash_command='echo "end matching"',
        )
        for country_code in ohub_country_codes:
            t = DatabricksSubmitRunOperator(
                task_id="match_{}_{}".format(self._dag_config.entity, country_code),
                cluster_name=self._dag_config.cluster_name,
                databricks_conn_id=self._databricks_conn_id,
                libraries=[{"egg": string_matching_egg}],
                spark_python_task={
                    "python_file": match_py,
                    "parameters": [
                        "--input_file",
                        ingest_input,
                        "--output_path",
                        self._intermediate_bucket.format(
                            date="{{ ds }}",
                            fn="{}_matched".format(self._dag_config.entity),
                        ),
                        "--country_code",
                        country_code,
                        "--threshold",
                        "0.9",
                    ],
                },
            )
            start_matching >> t >> end_matching

        return SubPipeline(start_matching, end_matching)

    def __delta_fuzzy_matching(
        self,
        delta_match_py: str,
        match_py: str,
        integrated_input: str,
        ingested_input: str,
        ohub_country_codes: List[str],
        string_matching_egg: str,
    ):
        start_matching = BashOperator(
            task_id=f"{self._dag_config.entity}_start_matching",
            bash_command='echo "start matching"',
        )
        end_matching = BashOperator(
            task_id=f"{self._dag_config.entity}_end_matching",
            bash_command='echo "end matching"',
        )

        for code in ohub_country_codes:
            match_new = DatabricksSubmitRunOperator(
                task_id=(
                    "match_new_{}_with_integrated_{}_{}".format(
                        self._dag_config.entity, self._dag_config.entity, code
                    )
                ),
                cluster_name=self._dag_config.cluster_name,
                databricks_conn_id=self._databricks_conn_id,
                libraries=[{"egg": string_matching_egg}],
                spark_python_task={
                    "python_file": delta_match_py,
                    "parameters": [
                        "--integrated_input_path",
                        integrated_input,
                        "--ingested_daily_input_path",
                        ingested_input,
                        "--updated_integrated_output_path",
                        self._intermediate_bucket.format(
                            date="{{ ds }}",
                            fn="{}_fuzzy_matched_delta_integrated".format(
                                self._dag_config.entity
                            ),
                        ),
                        "--unmatched_output_path",
                        self._intermediate_bucket.format(
                            date="{{ ds }}",
                            fn="{}_delta_left_overs".format(self._dag_config.entity),
                        ),
                        "--country_code",
                        code,
                    ],
                },
            )

            match_unmatched = DatabricksSubmitRunOperator(
                task_id="match_unmatched_{}_{}".format(self._dag_config.entity, code),
                cluster_name=self._dag_config.cluster_name,
                databricks_conn_id=self._databricks_conn_id,
                libraries=[{"egg": string_matching_egg}],
                spark_python_task={
                    "python_file": match_py,
                    "parameters": [
                        "--input_file",
                        self._intermediate_bucket.format(
                            date="{{ ds }}",
                            fn="{}_delta_left_overs".format(self._dag_config.entity),
                        ),
                        "--output_path",
                        self._intermediate_bucket.format(
                            date="{{ ds }}",
                            fn="{}_fuzzy_matched_delta".format(self._dag_config.entity),
                        ),
                        "--country_code",
                        code,
                    ],
                },
            )

            start_matching >> match_new >> match_unmatched >> end_matching

        return SubPipeline(start_matching, end_matching)

    def __ingest_from_channel(self, config: IngestConfig):
        dedup = "true" if config.dedup else "false"

        ingest_to_parquet = DatabricksSubmitRunOperator(
            task_id=f"{self._dag_config.entity}_{config.channel}_to_parquet",
            cluster_name=self._dag_config.cluster_name,
            databricks_conn_id=self._databricks_conn_id,
            libraries=[{"jar": self._spark_jobs_jar}],
            spark_jar_task={
                "main_class_name": f"com.unilever.ohub.spark.ingest.{config.channel}.{self._clazz}Converter",
                "parameters": [
                    "--inputFile",
                    config.input,
                    "--outputFile",
                    config.output,
                    "--fieldSeparator",
                    config.separator,
                    "--strictIngestion",
                    "false",
                    "--deduplicateOnConcatId",
                    dedup,
                ],
            },
        )

        return SubPipeline(ingest_to_parquet, ingest_to_parquet)

    def __export_acm_pipeline(
        self,
        integrated_bucket: str,
        filename: str,
        export_bucket: str,
        container_name: str,
        wasb_export_container: str,
        extra_acm_parameters: List[str],
    ):
        previous_integrated = (
            integrated_bucket.format(
                date="{{ yesterday_ds }}", fn=self._dag_config.entity
            )
            if self._dag_config.is_delta
            else None
        )
        delta_params = (
            ["--previousIntegrated", previous_integrated] if previous_integrated else []
        )

        convert_to_acm = DatabricksSubmitRunOperator(
            task_id=f"{self._dag_config.entity}_to_acm",
            cluster_name=self._dag_config.cluster_name,
            databricks_conn_id=self._databricks_conn_id,
            libraries=[{"jar": self._spark_jobs_jar}],
            spark_jar_task={
                "main_class_name": "com.unilever.ohub.spark.acm.{}AcmConverter".format(
                    self._clazz
                ),
                "parameters": [
                    "--inputFile",
                    integrated_bucket.format(date="{{ ds }}", fn=self._dag_config.entity),
                    "--outputFile",
                    export_bucket.format(date="{{ ds }}", fn=filename),
                ]
                + delta_params
                + extra_acm_parameters,
            },
        )

        tmp_file = f"/tmp/{filename}"

        # check_file_non_empty = CheckFileNonEmptyOperator(
        #     task_id=f'{self._dag_config.entity}_check_file_non_empty',
        #     file_path=tmp_file
        # )

        acm_from_wasb = FileFromWasbOperator(
            task_id=f"{self._dag_config.entity}_acm_from_wasb",
            file_path=tmp_file,
            container_name=container_name,
            wasb_conn_id="azure_blob",
            blob_name=wasb_export_container.format(date="{{ ds }}", fn=filename),
        )

        ftp_to_acm = SFTPOperator(
            task_id=f"{self._dag_config.entity}_ftp_to_acm",
            local_filepath=tmp_file,
            remote_filepath="/incoming/temp/ohub_2_test/{}".format(
                tmp_file.split("/")[-1]
            ),
            ssh_conn_id="acm_sftp_ssh",
            operation=SFTPOperation.PUT,
        )
        convert_to_acm >> acm_from_wasb >> ftp_to_acm

        return SubPipeline(convert_to_acm, ftp_to_acm)

    def __export_dispatch_pipeline(
        self,
        filename: str,
        integrated_bucket: str,
        export_bucket: str,
    ) -> SubPipeline:
        previous_integrated = (
            integrated_bucket.format(
                date="{{ yesterday_ds }}", fn=self._dag_config.entity
            )
            if self._dag_config.is_delta
            else None
        )
        delta_params = (
            ["--previousIntegrated", previous_integrated] if previous_integrated else []
        )
        convert_to_dispatch = DatabricksSubmitRunOperator(
            task_id=f"{self._dag_config.entity}_to_dispatcher_db",
            cluster_name=self._dag_config.cluster_name,
            databricks_conn_id=self._databricks_conn_id,
            libraries=[{"jar": self._spark_jobs_jar}],
            spark_jar_task={
                "main_class_name": "com.unilever.ohub.spark.dispatcher.{}DispatcherConverter".format(
                    self._clazz
                ),
                "parameters": [
                    "--inputFile",
                    integrated_bucket.format(date="{{ ds }}", fn=self._dag_config.entity),
                    "--outputFile",
                    export_bucket.format(date="{{ ds }}", fn=filename),
                ]
                + delta_params,
            },
        )
        return SubPipeline(convert_to_dispatch, convert_to_dispatch)


def slack_on_databricks_failure_callback(context):
    """
    Callback function to pass with DAG config

    :param context: Airflow context
    :return:
    """

    from airflow.operators.slack_operator import SlackAPIPostOperator
    from airflow.models import Variable

    log_link = "{base_url}/admin/airflow/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}".format(
        base_url=configuration.get("webserver", "BASE_URL"),
        dag_id=context["dag"].dag_id,
        task_id=context["task_instance"].task_id,
        execution_date=context["ts"],
    )
    if "databricks_url" in context:
        databricks_url = context["databricks_url"]
    else:
        databricks_url = "no url available"

    template = """
:skull: An airflow task failed at _{time}_
It looks like something went wrong with the task *{dag_id}.{task_id}* :anguished:. Better check the logs!
> <{airflow_log}|airflow logs>
> <{databricks_log}|databricks logs>
""".format(
        task_id=str(context["task"].task_id),
        dag_id=str(context["dag"].dag_id),
        time=str(context["ts"]),
        airflow_log=log_link,
        databricks_log=databricks_url,
    )

    slack_token = Variable.get("slack_airflow_token")
    operator = SlackAPIPostOperator(
        task_id="slack_failure_notification",
        token=slack_token,
        channel="#airflow",
        text=template,
    )

    return operator.execute(context=context)
