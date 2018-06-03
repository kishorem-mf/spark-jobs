from typing import List

from pyspark.sql.types import *
from string_matching import entity_delta_matching as victim
from string_matching import entity_matching as helper


class TestDeltaMatching(object):

    # note: both schema's (for integrated & delta) are exactly the same...and they should be

    schema_operators = StructType([
        StructField("concatId", StringType(), True),
        StructField("ohubId", StringType(), True),
        StructField("countryCode", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("houseNumber", StringType(), True),
        StructField("zipCode", StringType(), True),
    ])
    schema_contact_persons = StructType([
        StructField("concatId", StringType(), True),
        StructField("ohubId", StringType(), True),
        StructField("countryCode", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("mobileNumber", StringType(), True),
        StructField("emailAddress", StringType(), True),
        StructField("zipCode", StringType(), True),
        StructField("houseNumber", StringType(), True),
    ])

    def test_full_matching_operators(self, spark):
        delta_data = [
            ('c1', None, 'NL', 'xx', '', '', '', ''),
            ('c2', None, 'NL', 'xx', '', '', '', ''),
            ('c3', None, 'NL', 'xy', '', '', '', ''),
            ('c4', None, 'NL', 'xx', 'v', '', '', ''),
        ]
        integrated_data = [
            ('c5', 'o1', 'NL', 'xx', '', '', '', ''),
            ('c6', 'o2', 'NL', 'xx', '', '', '', ''),
            ('c7', 'o3', 'NL', 'xz', '', '', '', ''),
            ('c4', 'o4', 'NL', 'xx', '', '', '', ''),
        ]
        ingested = (spark.createDataFrame(delta_data, self.schema_operators)
                    .toDF('concatId', 'ohubId', 'countryCode', 'name', 'city', 'street', 'houseNumber', 'zipCode')
                    )
        integrated = (spark.createDataFrame(integrated_data, self.schema_operators)
                      .toDF('concatId', 'ohubId', 'countryCode', 'name', 'city', 'street', 'houseNumber', 'zipCode')
                      )

        updated, unmatched = victim.apply_delta_matching_on(spark,
                                                            ingested,
                                                            integrated,
                                                            helper.preprocess_operators,
                                                            victim.postprocess_delta_operators,
                                                            1500, 0.8)
        updated = updated.select('concatId', 'ohubId', 'city').sort('concatId').collect()
        unmatched = unmatched.select('concatId').collect()
        print(updated)
        print(unmatched)

        assert len(updated) == 6
        concats = [_[0] for _ in updated]
        ohubIds = [_[1] for _ in updated]
        cities_ = [_[2] for _ in updated]
        assert concats == ['c1', 'c2', 'c4', 'c5', 'c6', 'c7']
        assert ohubIds == ['o1', 'o1', 'o1', 'o1', 'o2', 'o3']
        assert cities_ == ['', '', 'v', '', '', '']

        assert len(unmatched) == 1
        assert unmatched[0][0] == 'c3'

    def test_full_matching_operators_replacing_group(self, spark):
        delta_data = [
            ('c1', None, 'NL', 'xx', 'nbar', '', '', ''),
            ('c2', None, 'NL', 'xx', 'nfoo', '', '', ''),
            ('c8', None, 'NL', 'aa', '', '', '', ''),
            ('c9', None, 'NL', 'bb', 'v', '', '', ''),
        ]
        integrated_data = [
            ('c1', 'o1', 'NL', 'xx', 'obar', '', '', ''),
            ('c2', 'o1', 'NL', 'xx', 'ofoo', '', '', ''),
            ('c3', 'o2', 'NL', 'xy', '', '', '', ''),
            ('c4', 'o3', 'NL', 'xz', '', '', '', ''),
        ]
        ingested = (spark.createDataFrame(delta_data, self.schema_operators)
                    .toDF('concatId', 'ohubId', 'countryCode', 'name', 'city', 'street', 'houseNumber', 'zipCode')
                    )
        integrated = (spark.createDataFrame(integrated_data, self.schema_operators)
                      .toDF('concatId', 'ohubId', 'countryCode', 'name', 'city', 'street', 'houseNumber', 'zipCode')
                      )

        updated, unmatched = victim.apply_delta_matching_on(spark,
                                                            ingested,
                                                            integrated,
                                                            helper.preprocess_operators,
                                                            victim.postprocess_delta_operators,
                                                            1500, 0.2)
        updated = updated.select('concatId', 'ohubId', 'city').sort('concatId').collect()

        unmatched = unmatched.select('concatId').sort('concatId').collect()

        assert len(updated) == 4
        concats = [_[0] for _ in updated]
        ohubIds = [_[1] for _ in updated]
        cities_ = [_[2] for _ in updated]
        assert concats == ['c1', 'c2', 'c3', 'c4']
        assert ohubIds == ['o1', 'o1', 'o2', 'o3']
        assert cities_ == ['nbar', 'nfoo', '', '']

        assert len(unmatched) == 2
        assert unmatched[0][0] == 'c8'
        assert unmatched[1][0] == 'c9'

    def test_full_matching_contact_persons(self, spark):
        delta_data = [
            ('c1', None, 'NL', 'x', 'x', '', 'street', None, None, '', ''),
            ('c2', None, 'NL', 'x', 'x', '', 'street', None, None, '', ''),
            ('c3', None, 'NL', 'x', 'y', '', 'street', None, None, '', ''),
            ('c4', None, 'NL', 'x', 'x', 'v', 'street', None, None, '', ''),
        ]
        integrated_data = [
            ('c5', 'o1', 'NL', 'x', 'x', '', 'street', None, None, '', ''),
            ('c6', 'o2', 'NL', 'x', 'x', '', 'street', None, None, '', ''),
            ('c7', 'o3', 'NL', 'x', 'z', '', 'street', None, None, '', ''),
            ('c4', 'o4', 'NL', 'x', 'x', '', 'street', None, None, '', ''),
        ]
        ingested = spark.createDataFrame(delta_data, self.schema_contact_persons)
        integrated = spark.createDataFrame(integrated_data, self.schema_contact_persons)

        updated, unmatched = victim.apply_delta_matching_on(spark,
                                                            ingested,
                                                            integrated,
                                                            helper.preprocess_contact_persons,
                                                            victim.postprocess_delta_contact_persons,
                                                            1500, 0.8)
        updated = updated.select('concatId', 'ohubId', 'city').sort('concatId').collect()
        unmatched = unmatched.select('concatId').collect()

        assert len(updated) == 6
        concats = [_[0] for _ in updated]
        ohubIds = [_[1] for _ in updated]
        cities_ = [_[2] for _ in updated]
        assert concats == ['c1', 'c2', 'c4', 'c5', 'c6', 'c7']
        assert ohubIds == ['o1', 'o1', 'o1', 'o1', 'o2', 'o3']
        assert cities_ == ['', '', 'v', '', '', '']

        assert len(unmatched) == 1
        assert unmatched[0][0] == 'c3'

    def test_full_matching_contact_persons_on_empty_delta(self, spark):
        delta_data = [
            ('c1', None, 'NL', 'x', 'x', '', '', None, None, '', ''),
            ('c2', None, 'NL', 'x', 'x', '', '', None, None, '', ''),
            ('c3', None, 'NL', 'x', 'y', '', '', None, None, '', ''),
            ('c4', None, 'NL', 'x', 'x', 'v', '', None, None, '', ''),
        ]
        integrated_data = [
            ('c5', 'o1', 'NL', 'x', 'x', '', 'street', None, None, '', ''),
            ('c6', 'o2', 'NL', 'x', 'x', '', 'street', None, None, '', ''),
            ('c7', 'o3', 'NL', 'x', 'z', '', 'street', None, None, '', ''),
            ('c4', 'o4', 'NL', 'x', 'x', '', 'street', None, None, '', ''),
        ]
        ingested = spark.createDataFrame(delta_data, self.schema_contact_persons)
        integrated = spark.createDataFrame(integrated_data, self.schema_contact_persons)

        updated, unmatched = victim.apply_delta_matching_on(spark,
                                                            ingested,
                                                            integrated,
                                                            helper.preprocess_contact_persons,
                                                            victim.postprocess_delta_contact_persons,
                                                            1500, 0.8)
        updated = updated.select('concatId', 'ohubId', 'city').sort('concatId').collect()
        unmatched = unmatched.select('concatId').collect()

        assert len(updated) == 4
        assert len(unmatched) == 4

    def test_full_matching_contact_persons_on_empty_integrated(self, spark):
        delta_data = [
            ('c1', None, 'NL', 'x', 'x', '', 'street', None, None, '', ''),
            ('c2', None, 'NL', 'x', 'x', '', 'street', None, None, '', ''),
            ('c3', None, 'NL', 'x', 'y', '', 'street', None, None, '', ''),
            ('c4', None, 'NL', 'x', 'x', 'v', 'street', None, None, '', ''),
        ]
        integrated_data = [
            ('c5', 'o1', 'NL', 'x', 'x', '', '', None, None, '', ''),
            ('c6', 'o2', 'NL', 'x', 'x', '', '', None, None, '', ''),
            ('c7', 'o3', 'NL', 'x', 'z', '', '', None, None, '', ''),
            ('c4', 'o4', 'NL', 'x', 'x', '', '', None, None, '', ''),
        ]
        ingested = spark.createDataFrame(delta_data, self.schema_contact_persons)
        integrated = spark.createDataFrame(integrated_data, self.schema_contact_persons)

        updated, unmatched = victim.apply_delta_matching_on(spark,
                                                            ingested,
                                                            integrated,
                                                            helper.preprocess_contact_persons,
                                                            victim.postprocess_delta_contact_persons,
                                                            1500, 0.8)
        updated = updated.select('concatId', 'ohubId', 'city').sort('concatId').collect()
        unmatched = unmatched.select('concatId').collect()

        assert len(updated) == 4
        assert len(unmatched) == 4
