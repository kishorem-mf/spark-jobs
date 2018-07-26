from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


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

        hook = WasbHook(wasb_conn_id=self._wasb_conn_id)
        # if no file
        if not hook.check_for_blob(self._container_name, self._file_path):
            # create empty one
            hook.load_string(
                "", self._container_name, self._file_path.replace("*", "empty")
            )
