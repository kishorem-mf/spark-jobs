from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperation
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults
import errno


class ShortCircuitSFTPOperator(BaseOperator, SkipMixin):
    """
    ShortCircuitSFTPOperator for transferring files from remote host to local or vice a versa. This operator uses
    ssh_hook to open sftp transport channel that serves as basis for file transfer.
    :param :class:`SSHHook` ssh_hook: predefined ssh_hook to use for remote execution
    :param str ssh_conn_id: connection id from airflow Connections
    :param str remote_host: remote host to connect
    :param str local_filepath: local file path to get or put
    :param str remote_filepath: remote file path to get or put
    :param str operation: specify operation 'get' or 'put'
    """

    template_fields = ("local_filepath", "remote_filepath")

    @apply_defaults
    def __init__(
        self,
        ssh_hook=None,
        ssh_conn_id: str = None,
        remote_host: str = None,
        local_filepath: str = None,
        remote_filepath: str = None,
        operation: str = SFTPOperation.PUT,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._ssh_hook = ssh_hook
        self._ssh_conn_id = ssh_conn_id
        self._remote_host = remote_host
        self._local_filepath = local_filepath
        self._remote_filepath = remote_filepath
        self._operation = operation

        if self._operation != SFTPOperation.GET and self._operation != SFTPOperation.PUT:
            raise ValueError(
                "Unsupported operation value {0}, expected {1} or {2}".format(
                    self._operation, SFTPOperation.GET, SFTPOperation.PUT
                )
            )

    def execute(self, context):
        file_msg = None
        try:
            if self._ssh_conn_id and not self._ssh_hook:
                self._ssh_hook = SSHHook(ssh_conn_id=self._ssh_conn_id)

            if not self._ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self._remote_host is not None:
                self._ssh_hook.remote_host = self._remote_host

            ssh_client = self._ssh_hook.get_conn()
            sftp_client = ssh_client.open_sftp()
            if self._operation == SFTPOperation.GET:
                try:
                    sftp_client.stat(self._remote_filepath)
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        self.log.info("Skipping downstream tasks...")
                        downstream_tasks = context["task"].get_flat_relatives(
                            upstream=False
                        )
                        self.log.debug(f"Downstream task_ids {downstream_tasks}")
                        if downstream_tasks:
                            self.skip(
                                context["dag_run"],
                                context["ti"].execution_date,
                                downstream_tasks,
                            )
                else:
                    file_msg = "From {0} to {1}".format(
                        self._remote_filepath, self._local_filepath
                    )
                    self.log.debug("Starting to transfer {file_msg}")
                    sftp_client.get(self._remote_filepath, self._local_filepath)
            else:
                file_msg = "From {0} to {1}".format(
                    self._local_filepath, self._remote_filepath
                )
                self.log.debug("Starting to transfer file {file_msg}")
                sftp_client.put(self._local_filepath, self._remote_filepath)

        except Exception as e:
            raise AirflowException(
                f"Error while transferring {file_msg}, error: {str(e)}"
            )

        return None
