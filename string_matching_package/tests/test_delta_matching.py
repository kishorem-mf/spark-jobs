from typing import List

from pyspark.sql.types import *
from string_matching import entity_delta_matching as victim
from string_matching import entity_matching as helper


class TestDeltaMatchingOperators(object):
    def create_ddf(self, spark, data, id_col):
        return

    def test_full_matching(self, spark):
        delta_data = [
            ('c1', 'NL', 'xx', '', '', '', ''),
            ('c2', 'NL', 'xx', '', '', '', ''),
            ('c3', 'NL', 'xy', '', '', '', ''),
            ('c4', 'NL', 'xx', 'v', '', '', ''),
        ]
        integrated_data = [
            ('c5', 'o1', 'NL', 'xx', '', '', '', ''),
            ('c6', 'o2', 'NL', 'xx', '', '', '', ''),
            ('c7', 'o3', 'NL', 'xz', '', '', '', ''),
            ('c4', 'o4', 'NL', 'xx', '', '', '', ''),
        ]
        ingested = (spark.createDataFrame(delta_data)
                    .toDF('concatId', 'countryCode', 'name', 'city', 'street', 'houseNumber', 'zipCode')
                    )
        integrated = (spark.createDataFrame(integrated_data)
                    .toDF('concatId', 'ohubId', 'countryCode', 'name', 'city', 'street', 'houseNumber', 'zipCode')
                    )

        updated, unmatched = victim.apply_delta_matching_on(spark,
                                       ingested,
                                       integrated,
                                       helper.preprocess_operators,
                                       victim.postprocess_operators,
                                       1500, 0.8)
        updated = updated.select('concatId', 'ohubId', 'city').sort('concatId').collect()
        unmatched = unmatched.select('concatId').collect()

        assert len(updated) == 6
        concats = [_[0] for _ in updated]
        ohubIds = [_[1] for _ in updated]
        cities_ = [_[2] for _ in updated]
        assert concats == ['c1', 'c2', 'c4', 'c5', 'c6', 'c7']
        assert ohubIds == ['o1', 'o1', 'o1', 'o1', 'o2', 'o3']
        assert cities_ == ['', '',  'v', '', '', '']

        assert len(unmatched) == 1
        assert unmatched[0][0] == 'c3'

