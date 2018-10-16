
from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestOperators(object):

    def test_full_matching_operators(self, spark):
        # raw contains 1000 records...TODO we currently loose 10 due to countryCode 'TW'

        assertDataframeCount(spark, "/usr/local/data/ingested/common/operators.parquet", 990)

        # integrated input is empty

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_fuzzy_matched_delta_integrated.parquet", 0)

        # fuzzy matching for DE only (so 90 records remain)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_delta_left_overs.parquet", 90)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_fuzzy_matched_delta.parquet", 57) # doubt this one

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_delta_golden_records.parquet", 90)

        assertDataframeCount(spark, "/usr/local/data/intermediate/operators_combined.parquet", 90)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/operators", 90)
