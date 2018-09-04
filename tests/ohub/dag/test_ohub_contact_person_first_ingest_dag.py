from testing.utils.test import test_dag, TestCase


class ContactInitialDagTestCase(TestCase):
    def test_ohub_contact_person_first_ingest_dag(self):
        from dags.ohub_contact_person_first_ingest_dag import dag
        test_dag(dag)
