from testing.utils.test import test_dag, TestCase


class ProductInitialDagTestCase(TestCase):
    def test_ohub_product_first_ingest_dag(self):
        from dags.ohub_product_first_ingest_dag import dag
        test_dag(dag)
