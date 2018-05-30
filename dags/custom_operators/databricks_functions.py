import logging
import time

from airflow.contrib.hooks.databricks_hook import DatabricksHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

LINE_BREAK = ('-' * 80)


def find_running_clusters_by_name(cluster_name,
                                  databricks_conn_id='databricks_default',
                                  databricks_hook=None):
    """
    Queries the Databricks API to find all running clusters with the given cluster name.
    :param cluster_name: the cluster name to look for.
    :param databricks_conn_id: the connection ID (default: `databricks_default`)
    :param databricks_hook: a `DatabricksHook` instance to use. Takes precedence over `databricks_conn_id`.
    :return: a list of ClusterInfo: https://docs.databricks.com/api/latest/clusters.html#clusterclusterinfo
    """
    hook = databricks_hook or DatabricksHook(databricks_conn_id=databricks_conn_id)
    body = hook._do_api_call(('GET', 'api/2.0/clusters/list'), {})

    non_terminated_states = ['PENDING', 'RUNNING', 'RESTARTING', 'RESIZING']
    return [cluster for cluster in body['clusters']
            if cluster_name == cluster['cluster_name'] and cluster['state'] in non_terminated_states]


def find_cluster_id(cluster_name,
                    databricks_conn_id='databricks_default',
                    databricks_hook=None):
    """
    Finds the `cluster_id` for a running cluster named `cluster_name`.
    :param cluster_name: the cluster name to look for.
    :param databricks_conn_id: the connection ID (default: `databricks_default`)
    :param databricks_hook: a `DatabricksHook` instance to use. Takes precedence over `databricks_conn_id`.
    :return: a cluster ID
    :raises AirflowException: if no matching cluster is found.
    """
    clusters = find_running_clusters_by_name(cluster_name, databricks_conn_id, databricks_hook)

    if len(clusters) == 0:
        raise AirflowException('Found no running Databricks cluster named "{}".'.format(cluster_name))
    elif len(clusters) > 1:
        logging.warning('Found more than one running Databricks cluster named "{}", using first match.', cluster_name)

    cluster_id = clusters[0]['cluster_id']
    return cluster_id


def get_cluster_status(cluster_id,
                       databricks_conn_id='databricks_default',
                       databricks_hook=None):
    """
    Requests databricks for the status of the cluster. For full specification of the return json,
    see https://docs.databricks.com/api/latest/clusters.html#get
    :param databricks_conn_id: the connection ID (default: `databricks_default`)
    :param databricks_hook: a `DatabricksHook` instance to use. Takes precedence over `databricks_conn_id`.
    :param hook: The hook into databricks
    :param cluster_id: The cluster id
    :return: The status of the cluster
    """

    hook = databricks_hook or DatabricksHook(databricks_conn_id=databricks_conn_id)
    body = hook._do_api_call(('GET', 'api/2.0/clusters/get?cluster_id={}'.format(cluster_id)), {})
    return body['state']


class BaseDatabricksOperator(BaseOperator):
    # Unilever blue under white text
    ui_color = '#0d009d'
    ui_fgcolor = '#fff'

    def __init__(self, databricks_conn_id, polling_period_seconds, databricks_retry_limit, **kwargs):
        super(BaseDatabricksOperator, self).__init__(**kwargs)
        self.json = {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit

    def get_hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit)


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
    template_fields = ('spark_jar_task', 'notebook_task', 'spark_python_task',)
    template_ext = ('.j2', '.jinja2',)

    def __init__(self,
                 existing_cluster_id=None,
                 spark_jar_task=None,
                 spark_python_task=None,
                 notebook_task=None,
                 libraries=None,
                 run_name=None,
                 timeout_seconds=None,
                 cluster_name=None,
                 databricks_conn_id='databricks_default',
                 polling_period_seconds=30,
                 databricks_retry_limit=3,
                 **kwargs):

        super(DatabricksSubmitRunOperator, self).__init__(databricks_conn_id,
                                                          polling_period_seconds,
                                                          databricks_retry_limit,
                                                          **kwargs)
        if cluster_name is not None and existing_cluster_id is not None:
            raise AirflowException('Cannot specify both cluster name and cluster id, choose one but choose wisely')
        self.cluster_name = cluster_name
        self.spark_jar_task = spark_jar_task
        self.spark_python_task = spark_python_task
        self.notebook_task = notebook_task
        self.run_page_url = None
        if libraries is not None:
            self.json['libraries'] = libraries
        if run_name is not None:
            self.json['run_name'] = run_name
        if existing_cluster_id is not None:
            self.json['existing_cluster_id'] = existing_cluster_id
        if timeout_seconds is not None:
            self.json['timeout_seconds'] = timeout_seconds
        if 'run_name' not in self.json:
            self.json['run_name'] = run_name or kwargs['task_id']

        # This variable will be used in case our task gets killed.
        self.run_id = None

    def _log_run_page_url(self, url):
        logging.info('View run status, Spark UI, and logs at {}'.format(url))

    def execute(self, context):
        if self.spark_jar_task is not None:
            self.json['spark_jar_task'] = self.spark_jar_task
        if self.notebook_task is not None:
            self.json['notebook_task'] = self.notebook_task
        if self.spark_python_task is not None:
            self.json['spark_python_task'] = self.spark_python_task
        hook = self.get_hook()
        if self.cluster_name is not None:
            cluster_id = find_cluster_id(self.cluster_name, databricks_hook=hook)
            logging.info('Using Databricks cluster_id "%s" for cluster named "%s"', cluster_id, self.cluster_name)
            self.json['existing_cluster_id'] = cluster_id

        self.run_id = hook.submit_run(self.json)
        self.run_page_url = hook.get_run_page_url(self.run_id)
        logging.info(LINE_BREAK)
        logging.info('Run submitted with run_id: {}'.format(self.run_id))
        self._log_run_page_url(self.run_page_url)
        context.update({'databricks_url': self.run_page_url})
        logging.info(LINE_BREAK)
        while True:
            run_state = hook.get_run_state(self.run_id)
            if run_state.is_terminal:
                if run_state.is_successful:
                    logging.info('{} completed successfully.'.format(
                        self.task_id))
                    self._log_run_page_url(self.run_page_url)
                    return
                else:
                    error_message = '{t} failed with terminal state: {s}'.format(
                        t=self.task_id,
                        s=run_state)
                    raise AirflowException(error_message)
            else:
                logging.info('{t} in run state: {s}'.format(t=self.task_id,
                                                            s=run_state))
                self._log_run_page_url(self.run_page_url)
                logging.info('Sleeping for {} seconds.'.format(
                    self.polling_period_seconds))
                time.sleep(self.polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        logging.info('Task: {t} with run_id: {r} was requested to be cancelled.'.format(
            t=self.task_id,
            r=self.run_id))


class DatabricksCreateClusterOperator(BaseDatabricksOperator):
    ui_color = '#d5ebc2'
    ui_fgcolor = '#000'

    template_fields = ('cluster_config',)

    def __init__(self,
                 cluster_config,
                 databricks_conn_id='databricks_default',
                 polling_period_seconds=30,
                 databricks_retry_limit=3,
                 **kwargs):
        super(DatabricksCreateClusterOperator, self).__init__(databricks_conn_id,
                                                              polling_period_seconds,
                                                              databricks_retry_limit,
                                                              **kwargs)
        self.cluster_config = cluster_config
        self.cluster_id = None

    def execute(self, context):
        hook = self.get_hook()
        logging.info('Creating new Databricks cluster with name "%s"', self.cluster_config['cluster_name'])

        body = hook._do_api_call(('POST', 'api/2.0/clusters/create'), self.cluster_config)
        self.cluster_id = body['cluster_id']

        while True:
            run_state = get_cluster_status(self.cluster_id, databricks_hook=hook)
            if run_state == 'RUNNING':
                logging.info('{} completed successfully.'.format(
                    self.task_id))
                return
            elif run_state == 'TERMINATED':
                error_message = 'Cluster creation failed with terminal state: {s}'.format(
                    s=run_state)
                raise AirflowException(error_message)
            else:
                logging.info('Cluster provisioning, currently in state: {s}'.format(s=run_state))
                logging.info('Sleeping for {} seconds.'.format(
                    self.polling_period_seconds))
                time.sleep(self.polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook._do_api_call(('POST', 'api/2.0/clusters/delete'), {'cluster_id': self.cluster_id})
        logging.info('Cluster creation with id: {c} was requested to be cancelled.'.format(c=self.cluster_id))


class DatabricksStartClusterOperator(BaseDatabricksOperator):
    ui_color = '#d5ebc2'
    ui_fgcolor = '#000'

    def __init__(self,
                 cluster_id,
                 databricks_conn_id='databricks_default',
                 polling_period_seconds=30,
                 databricks_retry_limit=3,
                 **kwargs):
        super(DatabricksStartClusterOperator, self).__init__(databricks_conn_id,
                                                             polling_period_seconds,
                                                             databricks_retry_limit,
                                                             **kwargs)
        self.cluster_id = cluster_id

    def execute(self, context):
        hook = self.get_hook()
        logging.info('Starting Databricks cluster with id %s"', self.cluster_id)

        run_state = get_cluster_status(self.cluster_id, databricks_hook=hook)
        if run_state == 'RUNNING':
            logging.info('Cluster already running.'.format(self.task_id))
            return

        hook._do_api_call(('POST', 'api/2.0/clusters/start'), {'cluster_id': self.cluster_id})
        while True:
            run_state = get_cluster_status(self.cluster_id, databricks_hook=hook)
            if run_state == 'RUNNING':
                logging.info('{} completed successfully.'.format(self.task_id))
                return
            elif run_state == 'TERMINATED':
                error_message = 'Cluster start failed with terminal state: {s}'.format(
                    s=run_state)
                raise AirflowException(error_message)
            else:
                logging.info('Cluster starting, currently in state: {s}'.format(s=run_state))
                logging.info('Sleeping for {} seconds.'.format(
                    self.polling_period_seconds))
                time.sleep(self.polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook._do_api_call(('POST', 'api/2.0/clusters/delete'), {'cluster_id': self.cluster_id})
        logging.info('Cluster start with id: {c} was requested to be cancelled.'.format(c=self.cluster_id))


class DatabricksTerminateClusterOperator(BaseDatabricksOperator):
    ui_color = '#ffc3bb'
    ui_fgcolor = '#000'

    def __init__(self,
                 cluster_name=None,
                 cluster_id=None,
                 databricks_conn_id='databricks_default',
                 polling_period_seconds=30,
                 databricks_retry_limit=3,
                 **kwargs):
        super(DatabricksTerminateClusterOperator, self).__init__(databricks_conn_id,
                                                                 polling_period_seconds,
                                                                 databricks_retry_limit,
                                                                 **kwargs)
        self.cluster_name = cluster_name
        self.cluster_id = cluster_id

    def execute(self, context):
        hook = self.get_hook()
        logging.info('Deleting Databricks cluster with name "%s"', self.cluster_name)

        if not self.cluster_id:
            self.cluster_id = find_cluster_id(self.cluster_name, databricks_hook=hook)
        hook._do_api_call(('POST', 'api/2.0/clusters/delete'), {'cluster_id': self.cluster_id})

        while True:
            run_state = get_cluster_status(self.cluster_id, self.databricks_conn_id)
            if run_state == 'TERMINATED':
                logging.info(
                    'Termination of cluster {} with id {} completed successfully.'.format(self.cluster_name,
                                                                                          self.cluster_id))
                return
            else:
                logging.info('Cluster terminating, currently in state: {s}'.format(s=run_state))
                logging.info('Sleeping for {} seconds.'.format(
                    self.polling_period_seconds))
                time.sleep(self.polling_period_seconds)


class DatabricksUninstallLibrariesOperator(BaseDatabricksOperator):
    ui_color = '#bbf3ff'
    ui_fgcolor = '#000'

    def __init__(self,
                 cluster_id,
                 libraries_to_uninstall,
                 databricks_conn_id='databricks_default',
                 polling_period_seconds=30,
                 databricks_retry_limit=3,
                 **kwargs):
        super(DatabricksUninstallLibrariesOperator, self).__init__(databricks_conn_id,
                                                                   polling_period_seconds,
                                                                   databricks_retry_limit,
                                                                   **kwargs)
        self.cluster_id = cluster_id
        self.libraries_to_uninstall = libraries_to_uninstall

    def execute(self, context):
        hook = self.get_hook()

        run_state = get_cluster_status(self.cluster_id, databricks_hook=hook)
        if run_state != 'RUNNING':
            raise AirflowException('Cluster must be running before removing libraries')

        logging.info('Restarting Databricks cluster with id %s"', self.cluster_id)

        hook._do_api_call(('POST', 'api/2.0/libraries/uninstall'), {'cluster_id': self.cluster_id,
                                                                    'libraries': self.libraries_to_uninstall})
        hook._do_api_call(('POST', 'api/2.0/clusters/restart'), {'cluster_id': self.cluster_id})

        time.sleep(10)  # make sure the API is refreshed, reflecting the restarting state
        while True:
            run_state = get_cluster_status(self.cluster_id, databricks_hook=hook)
            if run_state == 'RUNNING':
                logging.info('{} completed successfully.'.format(
                    self.task_id))
                return
            elif run_state == 'TERMINATED':
                error_message = 'Cluster start failed with terminal state: {s}'.format(
                    s=run_state)
                raise AirflowException(error_message)
            else:
                logging.info('Cluster restarting, currently in state: {s}'.format(s=run_state))
                logging.info('Sleeping for {} seconds.'.format(
                    self.polling_period_seconds))
                time.sleep(self.polling_period_seconds)

    def on_kill(self):
        hook = self.get_hook()
        hook._do_api_call(('POST', 'api/2.0/clusters/delete'), {'cluster_id': self.cluster_id})
        logging.info('Cluster start with id: {c} was requested to be cancelled.'.format(c=self.cluster_id))
