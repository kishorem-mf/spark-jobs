from testing.utils.test import test_dag, TestCase
from airflow import AirflowException
import pytest


class RedDagTestCase(TestCase):
    def test_dag_red(self):
        from ohub.fixtures.red_dag import dag
        with pytest.raises(AirflowException):
            test_dag(dag)
