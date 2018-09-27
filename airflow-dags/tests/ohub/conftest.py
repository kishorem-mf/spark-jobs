import datetime

import pytest
from airflow import DAG

DAG_DEFAULT_DATE = datetime.datetime(2018, 1, 1)
DAG_INTERVAL = datetime.timedelta(hours=12)


@pytest.fixture
def test_dag(tmpdir):
    """Returns test DAG instance."""

    return DAG(
        "test_dag",
        default_args={"owner": "airflow", "start_date": DAG_DEFAULT_DATE},
        template_searchpath=str(tmpdir),
        schedule_interval=DAG_INTERVAL,
    )
