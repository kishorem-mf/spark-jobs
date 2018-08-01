"""Tests the basic integrity of DAGs.

Based on example from https://github.com/danielvdende/data-testing-with-airflow/
"""

import glob
import sys
from os import path

import pytest
from airflow import models as af_models

DAG_PATH = path.join(path.dirname(__file__), "..", "..", "dags")
EXCLUDES = ["__init__.py", "config.py"]
DAG_FILES = [
    path.basename(file_path)
    for file_path in glob.glob(path.join(DAG_PATH, "*.py"))
    if path.basename(file_path) not in EXCLUDES
]


@pytest.mark.parametrize("dag_file", [pytest.param(dag_path) for dag_path in DAG_FILES])
def test_dag_integrity(dag_file):
    """Import DAG files and check for DAG."""
    module_name, _ = path.splitext(dag_file)
    module_path = path.join(DAG_PATH, dag_file)
    module = _import_file(module_name, module_path)
    assert any(isinstance(var, af_models.DAG) for var in vars(module).values())


def _import_file(module_name, module_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
