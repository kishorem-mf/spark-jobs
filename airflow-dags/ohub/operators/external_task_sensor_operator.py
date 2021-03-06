"""Sensor checking for task completion in a different DAG."""

import datetime
from time import sleep

from airflow import settings
from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator, TaskInstance
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults


# pylint: disable=too-many-instance-attributes
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
    :param execution_date_fn: function that receives the current execution date
        and returns the desired execution dates to query. Either execution_delta
        or execution_date_fn can be passed to ExternalTaskSensor, but not both.
    :type execution_date_fn: callable
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
        execution_date_fn=None,
        **kwargs
    ):

        super().__init__(**kwargs)
        self._poke_interval = poke_interval
        self._allowed_states = allowed_states or [State.SUCCESS]
        self._disallowed_states = allowed_states or [
            State.FAILED,
            State.UPSTREAM_FAILED,
        ]
        self._execution_delta = (
            datetime.timedelta(seconds=0) if not execution_delta else execution_delta
        )
        self._execution_date_fn = execution_date_fn
        self._external_dag_id = external_dag_id
        self._external_task_id = external_task_id
        self.succeeded_state = "succeeded"
        self.failed_state = "failed"
        self.running_state = "still_running"

    def poke(self, context):
        """
        Execute this after every interval.
        :param context:
        :return:
        """
        exec_date = context["execution_date"]
        dttm = (exec_date - self._execution_delta) if not self._execution_date_fn else self._execution_date_fn(exec_date)
        dttm_serialised = dttm.isoformat()

        self.log.info(f"Poking for {self._external_dag_id}.{self._external_task_id} on {dttm_serialised}")
        task_instance = TaskInstance

        session = settings.Session()
        allowed_count = (
            session.query(task_instance)
            .filter(
                task_instance.dag_id == self._external_dag_id,
                task_instance.task_id == self._external_task_id,
                task_instance.state.in_(self._allowed_states),
                task_instance.execution_date == dttm,
            )
            .count()
        )
        disallowed_count = (
            session.query(task_instance)
            .filter(
                task_instance.dag_id == self._external_dag_id,
                task_instance.task_id == self._external_task_id,
                task_instance.state.in_(self._disallowed_states),
                task_instance.execution_date == dttm,
            )
            .count()
        )
        session.close()

        retval = self.running_state
        if allowed_count == 1:
            retval = self.succeeded_state
        if disallowed_count == 1:
            retval = self.failed_state
        return retval

    def execute(self, context):
        while True:
            state = self.poke(context)
            if state == self.failed_state:
                raise AirflowSkipException(
                    "Snap. Task {} in DAG {} has failed :(".format(
                        self._external_task_id, self._external_dag_id
                    )
                )
            elif state == self.succeeded_state:
                self.log.info("Task {self._external_task_id} in DAG {self._external_dag_id} is successful")
                return
            sleep(self._poke_interval)
