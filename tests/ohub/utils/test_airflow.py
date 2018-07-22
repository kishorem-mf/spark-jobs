import pytest
from airflow.hooks.base_hook import BaseHook

from ohub.utils.airflow import LazyConnection


class TestLazyConnection(object):
    def test_no_connection_fetched(self, mocker):
        mock = mocker.patch.object(BaseHook, "get_connection")
        lazy_conn = LazyConnection(conn_id="test")
        mock.assert_not_called()

    def test_connection_fetched(self, mocker):
        mock = mocker.patch.object(BaseHook, "get_connection")
        lazy_conn = LazyConnection(conn_id="test")
        password = lazy_conn.conn.password
        mock.assert_called_once_with(conn_id="test")
