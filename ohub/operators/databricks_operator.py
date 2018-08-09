import logging
import time

from airflow import AirflowException
from airflow.contrib.hooks.databricks_hook import DatabricksHook
from airflow.models import BaseOperator

from ohub.utils.databricks import find_cluster_id, get_cluster_status

LINE_BREAK = "-" * 80


class BaseDatabricksOperator(BaseOperator):
    # Unilever blue under white text
    ui_color = "#0d009d"
    ui_fgcolor = "#fff"

    def __init__(
        self, databricks_conn_id, polling_period_seconds, databricks_retry_limit, **kwargs
    ):
        super().__init__(**kwargs)
        self._json = {}
        self._databricks_conn_id = databricks_conn_id
        self._polling_period_seconds = polling_period_seconds
        self._databricks_retry_limit = databricks_retry_limit

    def get_hook(self):
        return DatabricksHook(
            self._databricks_conn_id, retry_limit=self._databricks_retry_limit
        )


class DatabricksSubmitRunOperator(BaseDatabricksOperator):
    """
    Copied from `airflow.contrib.operators.databricks_operator.DatabricksSubmitRunOperator`.

    Changes:
    * Adds `cluster_name` constructor arg, which is used to look up the `existing_cluster_id` programmatically.

    Submits an Spark job run to Databricks using the
    `api/2.0/jobs/runs/submit
    <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_
    API endpoint.

    """

    template_fields = (
        "_spark_jar_task",
        "_notebook_task",
        "_spark_python_task",
        "_cluster_name",
    )
    template_ext = (".j2", ".jinja2")

    def __init__(
        self,
        existing_cluster_id=None,
        spark_jar_task=None,
        spark_python_task=None,
        notebook_task=None,
        libraries=None,
        run_name=None,
        timeout_seconds=None,
        cluster_name=None,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=3,
        **kwargs,
    ):

        super().__init__(databricks_conn_id, polling_period_seconds, databricks_retry_limit, **kwargs)
        if cluster_name is not None and existing_cluster_id is not None:
            raise AirflowException(
                "Cannot specify both cluster name and cluster id, choose one but choose wisely"
            )
        self._cluster_name = cluster_name
        self._spark_jar_task = spark_jar_task
        self._spark_python_task = spark_python_task
        self._notebook_task = notebook_task
        self._run_page_url = None
        if libraries is not None:
            self._json["libraries"] = libraries
        if run_name is not None:
            self._json["run_name"] = run_name
        if existing_cluster_id is not None:
            self._json["existing_cluster_id"] = existing_cluster_id
        if timeout_seconds is not None:
            self._json["timeout_seconds"] = timeout_seconds
        if "run_name" not in self._json:
            self._json["run_name"] = run_name or kwargs["task_id"]

        # This variable will be used in case our task gets killed.
        self.run_id = None

    @staticmethod
    def _log_run_page_url(url):
        logging.info(f"View run status, Spark UI, and logs at {url}")

    def execute(self, context):
        if self._spark_jar_task is not None:
            self._json["spark_jar_task"] = self._spark_jar_task
        if self._notebook_task is not None:
            self._json["notebook_task"] = self._notebook_task
        if self._spark_python_task is not None:
            self._json["spark_python_task"] = self._spark_python_task
        hook = self.get_hook()
        if self._cluster_name is not None:
            cluster_id = find_cluster_id(self._cluster_name, databricks_hook=hook)
            logging.info(
                f'Using Databricks cluster_id {cluster_id} for cluster named "{self._cluster_name}"'
            )
            self._json["existing_cluster_id"] = cluster_id

        self.run_id = hook.submit_run(self._json)
        self.run_page_url = hook.get_run_page_url(self.run_id)
        logging.info(LINE_BREAK)
        logging.info("Run submitted with run_id: {}".format(self.run_id))
        self._log_run_page_url(self.run_page_url)
        context.update({"databricks_url": self.run_page_url})
        logging.info(LINE_BREAK)
        while True:
            run_state = hook.get_run_state(self.run_id)
            if run_state.is_terminal:
                if run_state.is_successful:
                    logging.info("{} completed successfully.".format(self.task_id))
                    self._log_run_page_url(self.run_page_url)
                    return
                else:
                    error_message = "{t} failed with terminal state: {s}".format(t=self.task_id, s=run_state)
                    raise AirflowException(error_message)
            else:
                logging.info("{t} in run state: {s}".format(t=self.task_id, s=run_state))
                self._log_run_page_url(self.run_page_url)
                logging.info("Sleeping for {} seconds.".format(self._polling_period_seconds))
                time.sleep(self._polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        logging.info(
            "Task: {t} with run_id: {r} was requested to be cancelled.".format(
                t=self.task_id, r=self.run_id
            )
        )


class DatabricksCreateClusterOperator(BaseDatabricksOperator):
    ui_color = "#d5ebc2"
    ui_fgcolor = "#000"

    template_fields = ("cluster_config",)

    def __init__(
        self,
        cluster_config,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=3,
        **kwargs,
    ):
        super().__init__(databricks_conn_id, polling_period_seconds, databricks_retry_limit, **kwargs)
        self.cluster_config = cluster_config
        self.cluster_id = None

    def existing_cluster_running(self):
        hook = self.get_hook()
        cluster_already_running = False
        if self.cluster_config["reuse_cluster"]:
            try:
                self.cluster_id = find_cluster_id(self.cluster_config["cluster_name"], databricks_hook=hook)
                run_state = get_cluster_status(self.cluster_id, databricks_hook=hook)
                if run_state == "RUNNING":
                    cluster_already_running = True
                else:
                    hook._do_api_call(("POST", "api/2.0/clusters/delete"), {"cluster_id": self.cluster_id})
                    cluster_already_running = False
            except AirflowException:
                logging.info("Cluster not found or deleting cluster {} failed").format(self.cluster_id)
                cluster_already_running = False
        return cluster_already_running

    def execute(self, context):
        hook = self.get_hook()
        logging.info('Creating new Databricks cluster with name "%s"', self.cluster_config["cluster_name"])
        if not self.existing_cluster_running():
            body = hook._do_api_call(("POST", "api/2.0/clusters/create"), self.cluster_config)
            self.cluster_id = body["cluster_id"]

        while True:
            run_state = get_cluster_status(self.cluster_id, databricks_hook=hook)
            if run_state == "RUNNING":
                logging.info("{} completed successfully.".format(self.task_id))
                return
            elif run_state == "TERMINATED":
                error_message = "Cluster creation failed with terminal state: {s}".format(s=run_state)
                raise AirflowException(error_message)
            else:
                logging.info("Cluster provisioning, currently in state: {s}".format(s=run_state))
                logging.info("Sleeping for {} seconds.".format(self._polling_period_seconds))
                time.sleep(self._polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook._do_api_call(("POST", "api/2.0/clusters/delete"), {"cluster_id": self.cluster_id})
        logging.info("Cluster creation with id: {c} was requested to be cancelled.".format(c=self.cluster_id))


# See OHUB-1208
class DatabricksStartClusterOperator(BaseDatabricksOperator):
    ui_color = "#d5ebc2"
    ui_fgcolor = "#000"

    def __init__(
        self,
        cluster_id,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=3,
        **kwargs,
    ):
        super().__init__(databricks_conn_id, polling_period_seconds, databricks_retry_limit, **kwargs)
        self.cluster_id = cluster_id

    def execute(self, context):
        hook = self.get_hook()
        logging.info('Starting Databricks cluster with id %s"', self.cluster_id)

        run_state = get_cluster_status(self.cluster_id, databricks_hook=hook)
        if run_state == "RUNNING":
            logging.info("Cluster already running.")
            return

        hook._do_api_call(("POST", "api/2.0/clusters/start"), {"cluster_id": self.cluster_id})
        while True:
            run_state = get_cluster_status(self.cluster_id, databricks_hook=hook)
            if run_state == "RUNNING":
                logging.info("{} completed successfully.".format(self.task_id))
                return
            elif run_state == "TERMINATED":
                raise AirflowException("Cluster start failed with terminal state: {s}".format(s=run_state))
            else:
                logging.info("Cluster starting, currently in state: {s}".format(s=run_state))
                logging.info("Sleeping for {} seconds.".format(self._polling_period_seconds))
                time.sleep(self._polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook._do_api_call(("POST", "api/2.0/clusters/delete"), {"cluster_id": self.cluster_id})
        logging.info("Cluster start with id: {c} was requested to be cancelled.".format(c=self.cluster_id))


class DatabricksTerminateClusterOperator(BaseDatabricksOperator):
    ui_color = "#ffc3bb"
    ui_fgcolor = "#000"

    template_fields = ("cluster_name",)

    def __init__(
        self,
        cluster_name=None,
        cluster_id=None,
        cluster_config=None,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=3,
        **kwargs,
    ):
        super().__init__(
            databricks_conn_id, polling_period_seconds, databricks_retry_limit, **kwargs
        )
        self.cluster_name = cluster_name
        self.cluster_id = cluster_id
        self.cluster_config = cluster_config

    def execute(self, context):
        if not self.cluster_config["reuse_cluster"]:
            hook = self.get_hook()
            logging.info('Deleting Databricks cluster with name "%s"', self.cluster_name)

            if not self.cluster_id:
                self.cluster_id = find_cluster_id(self.cluster_name, databricks_hook=hook)
            hook._do_api_call(("POST", "api/2.0/clusters/delete"), {"cluster_id": self.cluster_id})

            while True:
                run_state = get_cluster_status(self.cluster_id, self._databricks_conn_id)
                if run_state == "TERMINATED":
                    logging.info(
                        "Termination of cluster %s with id %s completed successfully.",
                        self.cluster_name,
                        self.cluster_id)
                    return
                else:
                    logging.info("Cluster terminating, currently in state: %s", run_state)
                    logging.info("Sleeping for %s seconds.", self._polling_period_seconds)
                    time.sleep(self._polling_period_seconds)
