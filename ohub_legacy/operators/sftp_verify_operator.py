# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SftpVerifyOperator(BaseOperator):
    """
    SftpVerifyOperator for transferring files from remote host to local or vice a versa.
    This operator uses ssh_hook to open sftp transport channel that serves as basis
    for file transfer.
    :param ssh_hook: predefined ssh_hook to use for remote execution
    :type ssh_hook: :class:`SSHHook`
    :param ssh_conn_id: connection id from airflow Connections
    :type ssh_conn_id: str
    :param remote_host: remote host to connect
    :type remote_host: str
    :param remote_filepath: remote file path to get or put
    :type remote_filepath: str
    """
    template_fields = ('remote_filepath')

    @apply_defaults
    def __init__(self,
                 ssh_hook=None,
                 ssh_conn_id=None,
                 remote_host=None,
                 remote_filepath=None,
                 *args,
                 **kwargs):
        super(SftpVerifyOperator, self).__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.remote_filepath = remote_filepath

    def execute(self, context):
        try:
            if self.ssh_conn_id and not self.ssh_hook:
                self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("can not operate without ssh_hook or ssh_conn_id")

            if self.remote_host is not None:
                self.ssh_hook.remote_host = self.remote_host

            ssh_client = self.ssh_hook.get_conn()
            sftp_client = ssh_client.open_sftp()
            sftp_client.stat(self.remote_filepath)

        except Exception as e:
            raise AirflowException("Error while verifying {0}, error: {1}"
                                   .format(self.remote_filepath, str(e)))
        return None
