from testing.utils.test import test_dag, TestCase
from airflow import AirflowException
import pytest


class DagsTestCase(TestCase):

    # def test_dag_green(self):
    #     from ohub.fixtures.green_dag import dag
    #     test_dag(dag)

    # def test_dag_red(self):
    #     from ohub.fixtures.red_dag import dag
    #     with pytest.raises(AirflowException):
    #         test_dag(dag)

    # def test_fuzzit_import_dag(self):
    #     from dags.fuzzit_import_dag import dag
    #     test_dag(dag)

    # def test_ohub_contact_person_first_ingest_dag(self):
    #     from dags.ohub_contact_person_first_ingest_dag import dag
    #     test_dag(dag)

    # def test_ohub_contact_persons_dag(self):
    #     from dags.ohub_contact_persons_dag import dag
    #     test_dag(dag)

    # def test_ohub_operators_dag(self):
    #     from dags.ohub_operators_dag import dag
    #     test_dag(dag)

    # def test_ohub_operators_first_ingest_dag(self):
    #     from dags.ohub_operators_first_ingest_dag import dag
    #     test_dag(dag)

    # def test_ohub_orders_dag(self):
    #     from dags.ohub_orders_dag import dag
    #     test_dag(dag)

    # def test_ohub_orders_first_ingest_dag(self):
    #     from dags.ohub_orders_first_ingest_dag import dag
    #     test_dag(dag)

    # def test_ohub_product_first_ingest_dag(self):
    #     from dags.ohub_product_first_ingest_dag import dag
    #     test_dag(dag)

    # def test_ohub_products_dag(self):
    #     from dags.ohub_products_dag import dag
    #     test_dag(dag)

    # def test_ohub_subscription_first_ingest_dag(self):
    #     from dags.ohub_subscription_first_ingest_dag import dag
    #     test_dag(dag)

    def test_ohub_subscription(self):
        from dags.ohub_subscription import dag
        test_dag(dag)
