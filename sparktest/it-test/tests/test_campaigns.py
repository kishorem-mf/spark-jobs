import pytest
from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestCampaigns(object):

    @pytest.mark.skip(reason="pipline not yet implemented")
    def test_full_matching_campaigns(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/campaigns", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/campaigns.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/campaigns_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/campaigns", 1)
