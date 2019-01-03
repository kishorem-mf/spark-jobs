from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestCampaignBounces(object):

    def test_full_matching_campaign_bounces(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/campaign_bounces", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/campaign_bounces.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/campaign_bounces_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/campaign_bounces", 1)
