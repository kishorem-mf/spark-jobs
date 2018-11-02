from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestSubscriptions(object):

    def test_full_matching_subscriptions(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/subscriptions", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/subscriptions.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/subscriptions_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/subscriptions", 1)
