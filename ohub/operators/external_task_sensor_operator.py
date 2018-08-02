import datetime

from future import standard_library

from time import sleep

from airflow import settings
from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator, TaskInstance
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults

# TODO: Remove and test this
standard_library.install_aliases()


class ExternalTaskSensorOperator(BaseOperator):
    """
    Sensor operators are derived from this class an inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
        a criteria is met and fail if and when they time out.

    :param int poke_interval: Time in seconds that the job should wait in between each tries
    :param str external_dag_id: The dag_id that contains the task you want to wait for
    :param str external_task_id: The task_id that contains the task you want to wait for
    :param list allowed_states: list of allowed states, default is ``['success']``
    :param datetime.timedelta execution_delta: time difference with the previous execution to look at, the default is
        the same execution_date as the current task. For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to ExternalTaskSensor, but not both.
    :param Callable execution_date_fn: function that receives the current execution date and returns the desired
        execution dates to query. Either execution_delta or execution_date_fn can be passed to ExternalTaskSensor, but
        not both.
    """

    ui_color = "#19647e"
    ui_fgcolor = "#fff"

    @apply_defaults
    def __init__(
        self,
        external_dag_id,
        external_task_id,
        poke_interval=60,
        allowed_states=None,
        execution_delta=None,
        **kwargs
    ):

        super().__init__(**kwargs)
        self._poke_interval = poke_interval
        self._allowed_states = allowed_states or [State.SUCCESS]
        self._disallowed_states = allowed_states or [State.FAILED, State.UPSTREAM_FAILED]
        self._execution_delta = (
            datetime.timedelta(seconds=0) if not execution_delta else execution_delta
        )
        self._external_dag_id = external_dag_id
        self._external_task_id = external_task_id
        self.SUCCEEDED_STATE = "succeeded"
        self.FAILED_STATE = "failed"
        self.RUNNING_STATE = "still_running"

    def poke(self, context):
        dttm = context["execution_date"] - self._execution_delta
        dttm_serialised = dttm.isoformat()

        self.log.info(
            "Poking for "
            "{self.external_dag_id}."
            "{self.external_task_id} on "
            "{dttm_serialised}".format(**locals())  # TODO: replace locals() by something more sensible
        )
        TI = TaskInstance

        session = settings.Session()
        allowed_count = (
            session.query(TI)
            .filter(
                TI.dag_id == self._external_dag_id,
                TI.task_id == self._external_task_id,
                TI.state.in_(self._allowed_states),
                TI.execution_date == dttm,
            )
            .count()
        )
        disallowed_count = (
            session.query(TI)
            .filter(
                TI.dag_id == self._external_dag_id,
                TI.task_id == self._external_task_id,
                TI.state.in_(self._disallowed_states),
                TI.execution_date == dttm,
            )
            .count()
        )
        session.close()

        retval = self.RUNNING_STATE
        if allowed_count == 1:
            retval = self.SUCCEEDED_STATE
        if disallowed_count == 1:
            retval = self.FAILED_STATE
        return retval

    def execute(self, context):
        while True:
            state = self.poke(context)
            if state == self.FAILED_STATE:
                raise AirflowSkipException(
                    "Snap. Task {} in DAG {} has failed :(".format(
                        self._external_task_id, self._external_dag_id
                    )
                )
            elif state == self.SUCCEEDED_STATE:
                self.log.info(
                    "Task {} in DAG {} is successful".format(
                        self._external_task_id, self._external_dag_id
                    )
                )
                return
            sleep(self._poke_interval)
