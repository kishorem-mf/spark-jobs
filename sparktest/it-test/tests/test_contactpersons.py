
from pyspark.sql.types import *

class TestContactPersons(object):

    def test_full_matching_contact_persons(self, spark):
        # raw contains 1000 records...TODO we currently loose 10 due to countryCode 'TW'

        ingested = (spark
                    .read
                    .parquet("/usr/local/data/ingested/common/contactpersons.parquet")
                    )

        assert ingested.count() == 990

        # integrated input is empty

        pre_processed = (spark
                         .read
                         .parquet("/usr/local/data/intermediate/contactpersons_pre_processed.parquet")
                         )

        assert pre_processed.count() == 990


        exact_matches = (spark
                         .read
                         .parquet("/usr/local/data/intermediate/contactpersons_exact_matches.parquet")
                         )

        assert exact_matches.count() == 865

        unmatched_integrated = (spark
                                .read
                                .parquet("/usr/local/data/intermediate/contactpersons_unmatched_integrated.parquet")
                                )

        assert unmatched_integrated.count() == 0

        # fuzzy matching for TH only (so 122/125 records)

        unmatched_delta = (spark
                           .read
                           .parquet("/usr/local/data/intermediate/contactpersons_unmatched_delta.parquet")
                           )

        assert unmatched_delta.count() == 125

        fuzzy_matched_delta_integrated = (spark
                                          .read
                                          .parquet("/usr/local/data/intermediate/contactpersons_fuzzy_matched_delta_integrated.parquet")
                                          )

        assert fuzzy_matched_delta_integrated.count() == 0

        delta_left_overs = (spark
                            .read
                            .parquet("/usr/local/data/intermediate/contactpersons_delta_left_overs.parquet")
                           )

        assert delta_left_overs.count() == 122

        fuzzy_matched_delta = (spark
                               .read
                               .parquet("/usr/local/data/intermediate/contactpersons_fuzzy_matched_delta.parquet")
                              )

        assert fuzzy_matched_delta.count() == 55

        delta_golden_records = (spark
                                .read
                                .parquet("/usr/local/data/intermediate/contactpersons_delta_golden_records.parquet")
                               )

        assert delta_golden_records.count() == 122

        combined = (spark
                    .read
                    .parquet("/usr/local/data/intermediate/contactpersons_combined.parquet")
                   )

        assert combined.count() == 987

        references = (spark
                      .read
                      .parquet("/usr/local/data/intermediate/contactpersons_updated_references.parquet")
                     )

        assert references.count() == 987

        integrated_output = (spark
                             .read
                             .parquet("/usr/local/data/output/integrated/contactpersons")
                             )

        assert integrated_output.count() == 987
