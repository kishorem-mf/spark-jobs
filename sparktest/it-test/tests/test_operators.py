
from pyspark.sql.types import *

class TestOperators(object):

    def test_full_matching_operators(self, spark):
        # raw contains 1000 records...TODO we currently loose 10 due to countryCode 'TW'

        ingested = (spark
                    .read
                    .parquet("/usr/local/data/ingested/common/operators.parquet")
                    )

        assert ingested.count() == 990

        # integrated input is empty

        updated_integrated = (spark
                              .read
                              .parquet("/usr/local/data/intermediate/operators_fuzzy_matched_delta_integrated.parquet")
                              )

        assert updated_integrated.count() == 0

        # fuzzy matching for DE only (so 90 records remain)

        delta_left_overs = (spark
                            .read
                            .parquet("/usr/local/data/intermediate/operators_delta_left_overs.parquet")
                            )

        assert delta_left_overs.count() == 90

        fuzzy_matched_delta = (spark
                               .read
                               .parquet("/usr/local/data/intermediate/operators_fuzzy_matched_delta.parquet")
                               )

        assert fuzzy_matched_delta.count() == 57 # doubt this one

        delta_golden_records = (spark
                                .read
                                .parquet("/usr/local/data/intermediate/operators_delta_golden_records.parquet")
                                )
        assert delta_golden_records.count() == 90

        combined = (spark
                    .read
                    .parquet("/usr/local/data/intermediate/operators_combined.parquet")
                    )

        assert combined.count() == 90

        integrated_output = (spark
                             .read
                             .parquet("/usr/local/data/output/integrated/operators")
                             )

        assert integrated_output.count() == 90
