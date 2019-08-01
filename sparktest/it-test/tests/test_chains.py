from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestCampaigns(object):

    def test_full_matching_campaigns(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/chains", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/chains.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/chains_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/chains", 1)
