from custom_operators.wasb_hook import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class WasbCopyOperator(BaseOperator):
    """
    Copy a file in Azure Blob Storage.
    """
    template_fields = ('container_name', 'blob_name', 'copy_source')

    @apply_defaults
    def __init__(self, wasb_conn_id, container_name, blob_name, copy_source, *args, **kwargs):
        super(WasbCopyOperator, self).__init__(*args, **kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.copy_source = copy_source
        self.kwargs = kwargs

    def execute(self, context):
        """Copy a file in Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        hook.copy_blob(self.container_name, self.blob_name, self.copy_source)
