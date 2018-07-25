"""Tests for `ohub.utils.airflow`"""

from airflow.hooks.base_hook import BaseHook

from ohub.utils.airflow import LazyConnection


def test_no_connection_fetched(mocker):
    """Test if LazyConnection initialization results in call to get_connection()"""
    mock = mocker.patch.object(BaseHook, "get_connection")
    LazyConnection(conn_id="test")
    mock.assert_not_called()


def test_connection_fetched(mocker):
    """Test if calling LazyConnection attribute results in call to get_connection()"""
    mock = mocker.patch.object(BaseHook, "get_connection")
    lazy_conn = LazyConnection(conn_id="test")
    # pylint: disable=pointless-statement
    lazy_conn.password
    mock.assert_called_once_with(conn_id="test")
