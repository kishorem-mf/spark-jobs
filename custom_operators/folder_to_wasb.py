# adjusted from https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/operators/file_to_wasb.py

import os
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FolderToWasbOperator(BaseOperator):
    """
    Uploads a file to Azure Blob Storage.
    :param folder_path: Path to the file to load.
    :type folder_path: str
    :param container_name: Name of the container.
    :type container_name: str
    :param blob_name: Name of the blob.
    :type blob_name: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param load_options: Optional keyword arguments that
        `WasbHook.load_file()` takes.
    :type load_options: dict
    """
    template_fields = ('folder_path', 'container_name', 'blob_name')

    @apply_defaults
    def __init__(self, folder_path, container_name, blob_name,
                 wasb_conn_id='wasb_default', load_options=None, *args,
                 **kwargs):
        super(FileToWasbOperator, self).__init__(*args, **kwargs)
        if load_options is None:
            load_options = {}
        self.folder_path = folder_path
        self.container_name = container_name
        self.blob_name = blob_name
        self.wasb_conn_id = wasb_conn_id
        self.load_options = load_options

    def execute(self, context):
        """Upload a file to Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        self.log.info(
            'Uploading {self.folder_path} to wasb://{self.container_name} as {self.blob_name}'.format(**locals())
        )
        for file_path in os.listdir(self.folder_path):
            hook.load_file(file_path, self.container_name, self.blob_name, **self.load_options)
