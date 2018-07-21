from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator


class LazyConnection(object):
    """Lazy connection class that only fetches connection when accessed."""

    def __init__(self, conn_id):
        self._conn_id = conn_id
        self._conn = None

    def __getattr__(self):
        if self._conn is None:
            self._conn = BaseHook.get_connection(self._conn_id)
        return getattr(self, self._conn)


class SubPipeline(object):
    """A class holding the first and last task of a pipeline"""

    def __init__(self, first_task: BaseOperator, last_task: BaseOperator):
        self.first_task = first_task
        self.last_task = last_task
