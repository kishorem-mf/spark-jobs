import os
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class EmptyFallbackOperator(BaseOperator):
    """
    Uploads a file to Azure Blob Storage.
    :param container_name: Name of the container.
    :type container_name: str
    :param file_path: Path to the file to load.
    :type file_path: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    """
    template_fields = ('file_path', 'container_name')

    @apply_defaults
    def __init__(self, container_name, file_path,
                 wasb_conn_id='azure_blob', *args, **kwargs):
        super(EmptyFallbackOperator, self).__init__(*args, **kwargs)
        self.container_name = container_name
        self.wasb_conn_id = wasb_conn_id
        self.file_path = file_path

    def execute(self, context):
        """Create an empty placeholder file if there was no file yet already."""
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        # if no file
        if not hook.check_for_blob(self.container_name, self.file_path):
            # create empty one
            hook.load_string('',
                             self.container_name,
                             self.file_path.replace('*', 'empty'))
