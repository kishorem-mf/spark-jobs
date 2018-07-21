import os
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.utils.decorators import apply_defaults


class CheckFileNonEmptyOperator(ShortCircuitOperator):
    template_fields = ('file_path')

    @apply_defaults
    def __init__(self, file_path, *args, **kwargs):
        super(CheckFileNonEmptyOperator, self).__init__(
            python_callable=lambda: os.stat(self.file_path).st_size > 0,
            *args, **kwargs)
        self.file_path = file_path
