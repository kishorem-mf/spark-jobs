import pytest
from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestLoyaltyPoints(object):

    @pytest.mark.skip(reason="not yet implemented")
    def test_full_matching_loyalty_points(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/loyalty_points", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/loyalty_points.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/loyalty_points_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/loyalty_points", 1)
