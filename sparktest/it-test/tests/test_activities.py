from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestActivities(object):

    def test_full_matching_activities(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/activities", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/activities.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/activities_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/activities", 1)
