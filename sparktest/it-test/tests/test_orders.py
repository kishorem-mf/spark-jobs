from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestOrders(object):

    def test_full_matching_orders(self, spark):
        # raw contains 300 records (per sourceEntityId 3)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/orders", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/orders.parquet", 100)

        assertDataframeCount(spark, "/usr/local/data/intermediate/orders_pre_processed.parquet", 100)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/orders", 100)

        assertDataframeCount(spark, "/usr/local/data/input/integrated/orderlines", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/orderlines.parquet", 100)

        assertDataframeCount(spark, "/usr/local/data/intermediate/orderlines_pre_processed.parquet", 100)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/orderlines", 100)
