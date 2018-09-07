"""Utility functions for handling Databricks clusters"""

import logging
from typing import List

from airflow import AirflowException
from airflow.contrib.hooks.databricks_hook import DatabricksHook

DATABRICKS_POLLING_PERIOD_SECONDS = 30
DATABRICKS_RETRY_LIMIT = 3

def find_cluster_id(
    cluster_name: str,
    databricks_conn_id: str = "databricks_default",
    databricks_hook=None,
) -> str:
    """
    Finds the `cluster_id` for a running cluster named `cluster_name`.
    :param cluster_name: the cluster name to look for.
    :param databricks_conn_id: the connection ID (default: `databricks_default`)
    :param databricks_hook: a `DatabricksHook` instance to use. Takes precedence over `databricks_conn_id`.
    :return: a cluster ID
    :raises AirflowException: if no matching cluster is found.
    """

    clusters = find_running_clusters_by_name(
        cluster_name, databricks_conn_id, databricks_hook
    )

    if not clusters:
        raise AirflowException(
            f'Found no running Databricks cluster named "{cluster_name}".'
        )
    elif len(clusters) > 1:
        logging.warning(
            f'Found more than one running Databricks cluster named "{cluster_name}", using first match.'
        )

    cluster_id = clusters[0]["cluster_id"]
    return cluster_id


def find_running_clusters_by_name(
    cluster_name: str,
    databricks_conn_id: str = "databricks_default",
    databricks_hook=None,
) -> List:
    """
    Queries the Databricks API to find all running clusters with the given cluster name.
    :param cluster_name: the cluster name to look for.
    :param databricks_conn_id: the connection ID (default: `databricks_default`)
    :param databricks_hook: a `DatabricksHook` instance to use. Takes precedence over `databricks_conn_id`.
    :return: a list of ClusterInfo: https://docs.databricks.com/api/latest/clusters.html#clusterclusterinfo
    """

    hook = databricks_hook or DatabricksHook(databricks_conn_id=databricks_conn_id)
    # pylint: disable=protected-access
    # Will not fix this since it's a call to external code
    body = hook._do_api_call(("GET", "api/2.0/clusters/list"), {})
    # pylint: enable=protected-access

    non_terminated_states = ["PENDING", "RUNNING", "RESTARTING", "RESIZING"]
    return [
        cluster
        for cluster in body["clusters"]
        if cluster_name == cluster["cluster_name"]
        and cluster["state"] in non_terminated_states
    ]


def get_cluster_status(
    cluster_id: str, databricks_conn_id: str = "databricks_default", databricks_hook=None
):
    """
    Requests Databricks for the status of the cluster. For full specification of the return json,
    see https://docs.databricks.com/api/latest/clusters.html#get
    :param databricks_conn_id: the connection ID (default: `databricks_default`)
    :param databricks_hook: a `DatabricksHook` instance to use. Takes precedence over `databricks_conn_id`.
    :param hook: The hook into databricks
    :param cluster_id: The cluster id
    :return: The status of the cluster
    """

    hook = databricks_hook or DatabricksHook(databricks_conn_id=databricks_conn_id)
    # pylint: disable=protected-access
    # Will not fix this since it's a call to external code
    body = hook._do_api_call(
        ("GET", "api/2.0/clusters/get?cluster_id={}".format(cluster_id)), {}
    )
    # pylint: enable=protected-access
    return body["state"]
