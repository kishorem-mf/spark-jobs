from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SFTPOperator(BaseOperator):
    """
    SFTPOperator for transferring all files from a remote host to a destination
     folder.
    This operator uses ssh_hook to open sftp trasport channel that serve as
     basis for file transfer.

    :param ssh_hook: predefined ssh_hook to use for remote execution
    :type ssh_hook: :class:`SSHHook`
    :param ssh_conn_id: connection id from airflow Connections
    :type ssh_conn_id: str
    :param remote_host: remote host to connect
    :type remote_host: str
    :param destination_folder: file path to copy all files found on the server
     to
    :type local_filepath: str
    """
    template_fields = ('local_filepath', 'remote_filepath')

    @apply_defaults
    def __init__(self,
                 ssh_hook=None,
                 ssh_conn_id=None,
                 remote_host=None,
                 remote_folder='.',
                 destination_folder=None,
                 *args,
                 **kwargs):
        super(SFTPOperator, self).__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.remote_folder = remote_folder
        self.destination_folder = destination_folder

    def execute(self, context):
        file_msg = None
        try:
            if self.ssh_conn_id and not self.ssh_hook:
                self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException(
                    "can not operate without ssh_hook or ssh_conn_id")

            if self.remote_host is not None:
                self.ssh_hook.remote_host = self.remote_host

            ssh_client = self.ssh_hook.get_conn()
            sftp_client = ssh_client.open_sftp()

            for f in sftp_client.listdir(self.remote_folder):
                destination = self.destination_folder + '/' + f
                file_msg = "from {0} to {1}".format(f, destination)
                self.log.debug("Starting to transfer %s", file_msg)
                sftp_client.get(f, destination)

        except Exception as e:
            raise AirflowException("Error while transferring {0}, error: {1}"
                                   .format(file_msg, str(e)))

        return None
