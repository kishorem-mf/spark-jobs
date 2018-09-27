from testing.utils.test import test_dag, TestCase


class ProductInitialDagTestCase(TestCase):
    def test_ohub_products_first_ingest_dag(self):
        from dags.ohub_products_first_ingest_dag import dag
        test_dag(dag)
