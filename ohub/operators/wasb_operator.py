# Adjusted from https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/operators/file_to_wasb.py

import os
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FolderToWasbOperator(BaseOperator):
    """
    Upload a file to Azure Blob Storage.
    :param str folder_path: Path to the file to load.
    :param str container_name: Name of the container.
    :param str wasb_conn_id: Reference to the wasb connection.
    :param load_options: Optional keyword arguments that `WasbHook.load_file()` takes.
    :type load_options: dict
    """

    template_fields = ("_folder_path", "_container_name")

    @apply_defaults
    def __init__(
        self,
        folder_path,
        blob_name,
        container_name,
        wasb_conn_id="azure_blob",
        load_kwargs=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if load_kwargs is None:
            load_kwargs = {}
        self._folder_path = folder_path
        self._blob_name = blob_name
        self._container_name = container_name
        self._wasb_conn_id = wasb_conn_id
        self._load_kwargs = load_kwargs

    def execute(self, context):
        """Upload a file to Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self._wasb_conn_id)
        for file in os.listdir(self._folder_path):
            file_path = os.path.join(self._folder_path, file)
            remote_path = os.path.join(self._blob_name, file)
            self.log.info(f"Uploading {file_path} to wasb://{remote_path}")
            hook.load_file(
                file_path, self._container_name, remote_path, **self._load_kwargs
            )
