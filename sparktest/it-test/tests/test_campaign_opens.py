from test_utils import assertDataframeCount

class TestCampaignOpens(object):

    def test_full_matching_campaign_opens(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/campaign_opens", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/campaign_opens.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/campaign_opens_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/campaign_opens", 1)
