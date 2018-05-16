import pytest
import shutil
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request):
    """Fixture for creating a spark context

    :param request: pytest.FixtureRequest object
    :return: SparkSession
    """
    try:
        shutil.rmtree('metastore_db')
        shutil.rmtree('derby.log')
        shutil.rmtree('.cache')
    except OSError:
        pass
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    return spark
