
from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestContactPersons(object):

    def test_full_matching_contact_persons(self, spark):
        # raw contains 1000 records...TODO we currently loose 10 due to countryCode 'TW'

        assertDataframeCount(spark, "/usr/local/data/ingested/common/contactpersons.parquet", 990)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_pre_processed.parquet", 990)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_exact_matches.parquet", 865)

        # integrated input is empty
        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_unmatched_integrated.parquet", 0)

        # fuzzy matching for TH only (so 122/125 records)...is this correct?

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_unmatched_delta.parquet", 125)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_fuzzy_matched_delta_integrated.parquet", 0)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_delta_left_overs.parquet", 122)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_fuzzy_matched_delta.parquet", 55)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_delta_golden_records.parquet", 122)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_combined.parquet", 987)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_updated_references.parquet", 987)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/contactpersons", 987)
