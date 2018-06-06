from future import standard_library

standard_library.install_aliases()

from time import sleep

from airflow import settings
from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator, TaskInstance
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults


class ExternalTaskSensorOperator(BaseOperator):
    '''
    Sensor operators are derived from this class an inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
        a criteria is met and fail if and when they time out.

    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: string
    :param external_task_id: The task_id that contains the task you want to
        wait for
    :type external_task_id: string
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same execution_date as the current task.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :type execution_delta: datetime.timedelta
    :param execution_date_fn: function that receives the current execution date
        and returns the desired execution dates to query. Either execution_delta
        or execution_date_fn can be passed to ExternalTaskSensor, but not both.
    :type execution_date_fn: callable
    '''
    ui_color = '#19647e'

    @apply_defaults
    def __init__(
        self,
        external_dag_id,
        external_task_id,
        poke_interval=60,
        allowed_states=None,
        execution_delta=None,
        execution_date_fn=None,
        *args, **kwargs):
        super(ExternalTaskSensorOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval

        self.allowed_states = allowed_states or [State.SUCCESS]
        self.disallowed_states = allowed_states or [State.FAILED, State.UPSTREAM_FAILED]
        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                'Only one of `execution_date` or `execution_date_fn` may'
                'be provided to ExternalTaskSensor; not both.')

        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id

    def poke(self, context):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self.execution_date_fn(context['execution_date'])
        else:
            dttm = context['execution_date']

        dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        serialized_dttm_filter = ','.join(
            [datetime.isoformat() for datetime in dttm_filter])

        self.log.info(
            'Poking for '
            '{self.external_dag_id}.'
            '{self.external_task_id} on '
            '{} ... '.format(serialized_dttm_filter, **locals()))
        TI = TaskInstance

        session = settings.Session()
        allowed_count = session.query(TI).filter(
            TI.dag_id == self.external_dag_id,
            TI.task_id == self.external_task_id,
            TI.state.in_(self.allowed_states),
            TI.execution_date.in_(dttm_filter),
            ).count()
        disallowed_count = session.query(TI).filter(
            TI.dag_id == self.external_dag_id,
            TI.task_id == self.external_task_id,
            TI.state.in_(self.disallowed_states),
            TI.execution_date.in_(dttm_filter),
            ).count()
        session.close()

        retval = 'still_running'
        if allowed_count == len(dttm_filter):
            retval = 'succeeded'
        if disallowed_count == len(dttm_filter):
            retval = 'failed'
        return retval

    def execute(self, context):
        while True:
            state =  self.poke(context)
            if state == 'failed':
                raise AirflowSkipException('Snap. Task {} in DAG {} has failed :('.format(self.exernal_task_id,
                                                                                          self.external_dag_id))
            elif state == 'succeeded':
                self.log('Task {} in DAG {} has successful'.format(self.external_task_id, self.external_dag_id))
                return
            sleep(self.poke_interval)
