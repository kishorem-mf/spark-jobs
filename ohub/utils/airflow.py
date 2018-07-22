from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator


class LazyConnection(object):
    """Lazy connection class that only fetches connection when accessed.

    :param str conn_id: Airflow connection id.
    """

    def __init__(self, conn_id: str):
        self.conn = None
        self._conn_id = conn_id

    def __getattribute__(self, item):
        if object.__getattribute__(self, "conn") is None:
            object.__setattr__(
                self,
                "conn",
                BaseHook.get_connection(
                    conn_id=object.__getattribute__(self, "_conn_id")
                ),
            )
        return object.__getattribute__(self, item)


class SubPipeline(object):
    """A class holding the first and last task of a pipeline"""

    def __init__(self, first_task: BaseOperator, last_task: BaseOperator):
        self.first_task = first_task
        self.last_task = last_task
