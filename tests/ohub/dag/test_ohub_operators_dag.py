from testing.utils.test import test_dag, TestCase


class OperatorDeltaDagTestCase(TestCase):
    def test_ohub_operators_dag(self):
        from dags.ohub_operators_dag import dag
        test_dag(dag)
