from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestProducts(object):

    def test_full_matching_products(self, spark):
        # raw contains 100 records...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/products", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/products.parquet", 100)

        assertDataframeCount(spark, "/usr/local/data/intermediate/products_pre_processed.parquet", 100)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/products", 100)
