from testing.utils.test import test_dag, TestCase


class OperatorInitialDagTestCase(TestCase):
    def test_ohub_operators_first_ingest_dag(self):
        from dags.ohub_operators_first_ingest_dag import dag
        test_dag(dag)
