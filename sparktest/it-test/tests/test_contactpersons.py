
from pyspark.sql.types import *
from test_utils import assertDataframeCount
from pyspark.sql import functions as sf

class TestContactPersons(object):

    def test_full_matching_contact_persons(self, spark):
        # raw contains 1000 records...

        assertDataframeCount(spark, "/usr/local/data/ingested/common/contactpersons.parquet", 1000)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_pre_processed.parquet", 1000)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_exact_matches.parquet", 936)

        # integrated input is empty
        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_unmatched_integrated.parquet", 407)

        # fuzzy matching for TH only (so 121/132 records)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_unmatched_delta.parquet", 64)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_fuzzy_matched_delta_integrated.parquet", 59)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_delta_left_overs.parquet", 54)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_fuzzy_matched_delta.parquet", 18)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_delta_golden_records.parquet", 54)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_combined.parquet", 990)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_updated_references.parquet", 990)

        assertDataframeCount(spark, "/usr/local/data/intermediate/contactpersons_updated_valid_email.parquet", 990)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/contactpersons", 990)

        assert (spark
                .read
                .parquet("/usr/local/data/output/integrated/contactpersons")
                ).select('ohubId').distinct().count() == 443

        # 469 ohubIds from exact matches, 75 from fuzzy matching, 544 ohubIds in total
        assert(spark
               .read
               .parquet("/usr/local/data/output/integrated/contactpersons")
               ).filter(sf.isnull(sf.col('emailAddress')) & sf.isnull(sf.col('mobileNumber'))).select('ohubId').distinct().count() == 90
