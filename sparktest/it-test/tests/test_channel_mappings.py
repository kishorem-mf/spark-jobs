from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestChannelMapping(object):

    def test_full_matching_channel_mappings(self, spark):
        # raw contains 4 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/channel_mappings", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/channel_mappings.parquet", 4)

        assertDataframeCount(spark, "/usr/local/data/intermediate/channel_mappings_pre_processed.parquet", 4)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/channel_mappings", 4)
