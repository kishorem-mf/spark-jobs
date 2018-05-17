from typing import List

from pyspark.sql.types import *
from string_matching import entity_delta_matching as victim
from string_matching import entity_matching as helper


class TestDeltaMatchingOperators(object):
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                'zipCode')

    def test_full_matching(self, spark):
        delta_data = [
            ('1', 'NL', 'xx', 'city', 'street', 'number', 'zip'),
            ('2', 'NL', 'xx', 'city', 'street', 'number', 'zip'),
            ('3', 'NL', 'xy', 'city', 'street', 'number', 'zip'),
        ]
        integrated_data = [
            ('1', 'NL', 'xx', 'city', 'street', 'number', 'zip'),
            ('2', 'NL', 'xx', 'city', 'street', 'number', 'zip'),
            ('3', 'NL', 'xy', 'city', 'street', 'number', 'zip'),
        ]
        ingested = self.create_ddf(spark, data)
        integrated = self.create_ddf(spark, data)
        res = victim.apply_matching_on(spark,
                                       ingested,
                                       integrated,
                                       helper.preprocess_operators,
                                       victim.postprocess_operators,
                                       1500, 0.8).select('sourceId', 'targetId').sort('targetId').collect()
        assert len(res) == 2
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['1', '1']
        assert targets == ['2', '3']

