from custom_operators.wasb_hook import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json


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

        print("self.wasb_conn_id")
        print(self.wasb_conn_id)

        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        # print("hook")
        # print(json.dumps(vars(hook)))

        print("self.container_name")
        print(self.container_name)

        print("self.copy_source")
        print(self.copy_source)

        print("confirming source")
        assert hook.check_for_blob(self.container_name, self.copy_source)

        print("copying " + self.copy_source + " in container " + self.container_name + " to " + self.blob_name)
        hook.copy_blob(self.container_name, self.blob_name, self.copy_source)

        print("confirming destination")
        assert hook.check_for_blob(self.container_name, self.blob_name)
        print("confirmed destination")
