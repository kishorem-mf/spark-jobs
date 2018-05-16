# from: https://github.com/apache/incubator-airflow/blob/master/
# airflow/contrib/operators/file_to_wasb.py
import os

from custom_operators.wasb_hook import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FileFromWasbOperator(BaseOperator):
    """
    Downloads a file from Azure Blob Storage.
    :param file_path: Path to put the file to download.
    :type file_path: str
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
    template_fields = ('file_path', 'container_name', 'blob_name')

    @apply_defaults
    def __init__(self, file_path, container_name, blob_name,
                 wasb_conn_id='wasb_default', load_options=None, *args,
                 **kwargs):
        super(FileFromWasbOperator, self).__init__(*args, **kwargs)
        if load_options is None:
            load_options = {}
        self.file_path = file_path
        self.container_name = container_name
        self.blob_name = blob_name
        self.wasb_conn_id = wasb_conn_id
        self.load_options = load_options

    def execute(self, context):
        """Upload a file to Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)

        dir = '/'.join(self.file_path.split('/')[:-1])
        if not os.path.exists(dir):
            os.makedirs(dir)

        self.log.info(
            'Downloading {self.file_path} from {self.blob_name} on wasb://{self.container_name}'.format(**locals())
        )
        hook.get_file(self.file_path, self.container_name, self.blob_name, **self.load_options)
