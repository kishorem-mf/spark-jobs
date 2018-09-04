from testing.utils.test import test_dag, TestCase


class ProductDeltaDagTestCase(TestCase):
    def test_ohub_products_dag(self):
        from dags.ohub_products_dag import dag
        test_dag(dag)
