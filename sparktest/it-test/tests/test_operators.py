from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestOperators(object):

    def test_full_matching_operators(self, spark):
        # raw contains 1000 records...

        assertDataframeCount(spark, "/usr/local/data/ingested/common/operators.parquet", 1000)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_pre_processed.parquet", 1000)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_stitch_id.parquet", 1000)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_exact_matches.parquet", 1000)

        # integrated input is 769 as the reference records have to be passed to fuzzy logic
        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_unmatched_integrated.parquet", 733)

        # fuzzy matching for DE only (so 90 records remain)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_unmatched_delta.parquet", 0)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_fuzzy_matched_delta_integrated.parquet", 63)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_delta_left_overs.parquet", 0)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_fuzzy_matched_delta.parquet", 0)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_delta_golden_records.parquet", 0)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_combined.parquet", 1000)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/operators", 1000)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/operators_golden_ufs", 733)

        # 46 unique ohubId's for DE
        assert (spark
                .read
                .parquet("/usr/local/data/output/integrated/operators")
                ).select('ohubId').distinct().count() == 733
