from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestOperators(object):

    def test_full_matching_operators(self, spark):
        # raw contains 1000 records...

        assertDataframeCount(spark, "/usr/local/data/ingested/common/operators.parquet", 1000)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_pre_processed.parquet", 1000)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_exact_matches.parquet", 930)

        # integrated input is empty
        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_unmatched_integrated.parquet", 720)

        # fuzzy matching for DE only (so 90 records remain)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_unmatched_delta.parquet", 70)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_fuzzy_matched_delta_integrated.parquet", 67)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_delta_left_overs.parquet", 5)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_fuzzy_matched_delta.parquet", 2)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_delta_golden_records.parquet", 5)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_combined.parquet", 935)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/operators", 935)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/operators_golden", 723)

        # 46 unique ohubId's for DE
        assert (spark
                .read
                .parquet("/usr/local/data/output/integrated/operators")
                ).select('ohubId').distinct().count() == 723

