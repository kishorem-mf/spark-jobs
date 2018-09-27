from testing.utils.test import test_dag, TestCase


class OperatorDeltaDagTestCase(TestCase):
    def test_ohub_operators_dag(self):
        from dags.ohub_operators_first_ingest_dag import dag as initial
        from dags.ohub_operators_dag import dag as delta
        test_dag(initial)
        test_dag(delta)
