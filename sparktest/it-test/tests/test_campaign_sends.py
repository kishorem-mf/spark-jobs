import pytest
from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestCampaignSends(object):

    @pytest.mark.skip(reason="pipline not yet implemented")
    def test_full_matching_campaign_sends(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/campaign_sends", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/campaign_sends.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/campaign_sends_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/campaign_sends", 1)
