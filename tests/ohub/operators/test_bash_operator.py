from testing.utils.test import make_dag, test_dag, TestCase
from airflow.operators.bash_operator import BashOperator


class BashOperatorTestCase(TestCase):
    def test_bash_operator(self):
        import tempfile
        with tempfile.NamedTemporaryFile() as f:

            with make_dag('test_bash_dag') as dag:
                BashOperator(task_id='bash', bash_command=f'echo 1 > {f.name};')
                test_dag(dag)

            with open(f.name, 'r') as fr:
                self.assertIn('1', ''.join(fr.readlines()))
