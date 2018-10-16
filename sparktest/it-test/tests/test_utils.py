
from pyspark.sql import SparkSession

def assertDataframeCount(spark: SparkSession, fn: str, count: int):

    ddf = (spark
           .read
           .parquet(fn)
          )

    assert ddf.count() == count
