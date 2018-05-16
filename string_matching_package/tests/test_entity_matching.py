from typing import List

import pytest
from string_matching import entity_matching as victim


class TestPreprocessingOperators(object):

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo'),
                    ('2', 'NL', 'Dave Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '5312BE'),
                    ('3', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB'),
                    ('4', 'DE', 'Bruce Dickinson', 'Utrecht', 'Accacia Avenue', '22', '6666XX'), ]

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('concatId', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                     'zipCode')

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark)
        res = victim.preprocess_operators(ddf, 'concatId', True).collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = victim.preprocess_operators(ddf, 'concatId', True).select('matching_string').collect()
        assert res[0][0] == 'dave mustaine amsterdam baravenue14b 5312be'


class TestPreprocessingContactPersons(object):

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo', 'foo', 'phone', 'email'),
                    ('2', 'NL', 'Dave', 'Mustaine ', 'Amsterdam  ', '@barAvenue', '\u09F214b', '5312BE', None, None),
                    ('3', 'NL', 'Ritchie', 'Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB', None, None),
                    ('4', 'DE', 'Bruce', 'Dickinson', 'Utrecht', 'Accacia Avenue', '22', '6666XX', None, None),
                    ('5', 'DE', None, None, 'bar', 'bar', '43', '1234AB', None, None),
                    ('6', 'DE', '', '', 'bar', 'bar', '43', '1234AB', None, None),
                    ('7', 'DE', 'firstName', 'emptyStreet', 'bar', '', None, '1234AB', None, None),
                    ]

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('concatId', 'countryCode', 'firstName', 'lastName',
                                                     'city', 'street', 'houseNumber',
                                                     'zipCode', 'mobilePhoneNumber', 'emailAddress')

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark)
        res = victim.preprocess_contact_persons(ddf, 'concatId').collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = victim.preprocess_contact_persons(ddf, 'concatId').select('matching_string', 'streetCleansed').collect()
        assert res[0][0] == 'dave mustaine'
        assert res[0][1] == 'baravenue14b'


def gen_tuple(tup, n):
    return [(str(i),) + tup[1:-1] + (tup[-1].format(idx=i),) for i in range(n)]


class TestMatchingOperators(object):
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('1', 'NL', 'Dave Mustaine', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '1234AB'),
            ('2', 'NL', 'Dave Mustaine', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '1234AB'),
            ('3', 'NL', 'Dave Mustaien', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '1234AB'),
            ('4', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB')
        ]
        ddf = self.create_ddf(spark, data)
        res = victim.apply_matching_on(ddf, spark,
                                       victim.preprocess_operators,
                                       victim.postprocess_operators,
                                       'NL', 1500, 0.8).select('sourceId', 'targetId').sort('targetId').collect()
        assert len(res) == 2
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['1', '1']
        assert targets == ['2', '3']


class TestMatchingContactPersons(object):

    def create_ddf(self, spark, data: List[tuple]):
        return (spark.createDataFrame(data)
                .toDF('concatId', 'countryCode', 'firstName', 'lastName',
                      'city', 'street', 'mobilePhoneNumber', 'emailAddress',
                      'zipCode', 'houseNumber'))

    def test_full_matching(self, spark):
        data = [
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '14b'),
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '14b'),
            ('3', 'NL', 'Dave', 'Mustaire', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '14b'),
            ('1000', None, None, 'foo', 'foo', 'foo', 'phone', 'email', 'foo', 'foo'),
            ('1003', 'NL', 'Ritchie', 'Blackmore', 'Utrecht', 'fooStreet', '1234AB', None, None, '8'),
            ('1004', 'DE', 'Bruce', 'Dickinson', 'Utrecht', 'Accacia Avenue', '6666XX', None, None, '22'),
            ('1005', 'DE', None, None, 'bar', 'bar', '1234AB', None, None, '43'),
            ('1006', 'DE', '', '', 'bar', 'bar', '1234AB', None, None, '43'),
            ('1007', 'DE', 'firstName', 'emptyStreet', 'bar', '', '1234AB', None, None, None)
        ]
        ddf = self.create_ddf(spark, data)
        res = victim.apply_matching_on(ddf, spark,
                                       victim.preprocess_contact_persons,
                                       victim.postprocess_contact_persons,
                                       'NL', 1500, 0.8).select('sourceId', 'targetId').sort('targetId').collect()
        assert len(res) == 1
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['1']
        assert targets == ['2']
