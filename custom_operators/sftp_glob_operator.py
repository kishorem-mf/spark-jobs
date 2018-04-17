import re
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException


class SFTPGlobOperator(SFTPOperator):
    '''an SFTP operator for batch downloading files matching a glob'''

    def execute(self, context):
        file_msg = None
        try:
            if self.ssh_conn_id and not self.ssh_hook:
                self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
            if not self.ssh_hook:
                raise AirflowException("can not operate without ssh_hook or ssh_conn_id")
            if self.remote_host is not None:
                self.ssh_hook.remote_host = self.remote_host
            ssh_client = self.ssh_hook.get_conn()
            sftp_client = ssh_client.open_sftp()
            patt = re.compile(self.remote_filepath)
            # "a/b/c" -> "a/b"
            path = "/".join(self.remote_filepath.split("/")[:-1])
            for filename in sftp_client.listdir(path):
                if patt.match(filename):
                    file_msg = "from {0} to {1}".format(filename,
                                                        self.local_filepath)
                    self.log.debug("Starting to transfer %s", file_msg)
                    sftp_client.get(filename, self.local_filepath)
        except Exception as e:
            raise AirflowException("Error while transferring {0}, error: {1}"
                                   .format(file_msg, str(e)))
        return None
