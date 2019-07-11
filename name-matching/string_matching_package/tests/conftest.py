import shutil

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request):
    """
    Fixture for creating a Spark context

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
        .master("local[*]")
        .appName("pytest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    return spark
