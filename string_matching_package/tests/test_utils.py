from pyspark import sql
from pyspark.sql import functions as sf

from string_matching import utils


def test_get_countries(spark):
    ddf = spark.createDataFrame([(1, 2)])
    assert 1 == 2
