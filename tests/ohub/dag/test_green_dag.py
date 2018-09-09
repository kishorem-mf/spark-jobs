from testing.utils.test import test_dag, TestCase


class GreenDagTestCase(TestCase):
    def test_dag_green(self):
        from ohub.fixtures.green_dag import dag
        test_dag(dag)
