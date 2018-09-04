from testing.utils.test import test_dag, TestCase


class SubscriptionInitialDagTestCase(TestCase):
    def test_ohub_subscription_first_ingest_dag(self):
        from dags.ohub_subscription_first_ingest_dag import dag
        test_dag(dag)
