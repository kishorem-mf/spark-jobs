# Adjusted from https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/operators/file_to_wasb.py

import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ohub.vendor.airflow.contrib.hooks.wasb_hook import WasbHook


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


class WasbCopyOperator(BaseOperator):
    """
    Copy a file in Azure Blob Storage.
    """

    template_fields = ("_container_name", "_blob_name", "_copy_source")

    @apply_defaults
    def __init__(self, wasb_conn_id, container_name, blob_name, copy_source, **kwargs):
        super().__init__(**kwargs)
        self._wasb_conn_id = wasb_conn_id
        self._container_name = container_name
        self._blob_name = blob_name
        self._copy_source = copy_source
        self._kwargs = kwargs

    def execute(self, context):
        """Copy a file in Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self._wasb_conn_id)
        self.log.info(f"copying {self._copy_source} in container {self._container_name} to {self._blob_name}")
        idx = self._copy_source.find('data')
        for child in hook.connection.list_blobs(self._container_name, self._copy_source[idx:]):
            dest = self._blob_name + child.name[child.name.rfind('/'):]
            hook.copy_blob(self._container_name, dest, self._copy_source[:idx] + child.name)


class FileFromWasbOperator(BaseOperator):
    """
    Downloads a file from Azure Blob Storage.
    :param str file_path: Path to put the file to download.
    :param str container_name: Name of the container.
    :param str blob_name: Name of the blob.
    :param str wasb_conn_id: Reference to the wasb connection.
    :param dict load_options: Optional keyword arguments that `WasbHook.load_file()` takes.
    """

    template_fields = ("_file_path", "_container_name", "_blob_name")

    @apply_defaults
    def __init__(
        self,
        file_path,
        container_name,
        blob_name,
        wasb_conn_id="wasb_default",
        load_options=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if load_options is None:
            load_options = {}
        self._file_path = file_path
        self._container_name = container_name
        self._blob_name = blob_name
        self._wasb_conn_id = wasb_conn_id
        self._load_options = load_options

    def execute(self, context):
        """Upload a file to Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self._wasb_conn_id)

        dir = "/".join(self._file_path.split("/")[:-1])
        if not os.path.exists(dir):
            os.makedirs(dir)

        self.log.info(
            f"Downloading {self._file_path} from {self._blob_name} on wasb://{self._container_name}"
        )
        hook.get_file(
            self._file_path, self._container_name, self._blob_name, **self._load_options
        )


class EmptyFallbackOperator(BaseOperator):
    """
    Uploads a file to Azure Blob Storage.

    :param str container_name: Name of the container.
    :param str file_path: Path to the file to load.
    :param str wasb_conn_id: Reference to the wasb connection.
    """

    template_fields = ("_file_path", "_container_name")

    @apply_defaults
    def __init__(self, container_name, file_path, wasb_conn_id="azure_blob", **kwargs):
        super().__init__(**kwargs)
        self._container_name = container_name
        self._file_path = file_path
        self._wasb_conn_id = wasb_conn_id

    def execute(self, context):
        """Create an empty placeholder file if there was no file yet already."""

        self.log.info(f"ensuring availability of {self._file_path}")
        hook = WasbHook(wasb_conn_id=self._wasb_conn_id)
        if not hook.check_for_blob(self._container_name, self._file_path):
            fpath = self._file_path.replace("*", "empty")
            self.log.info(f"not present yet, creating empty {fpath}")
            hook.load_string("", self._container_name, fpath)
        else:
            self.log.info(f"{self._file_path} already exists")
