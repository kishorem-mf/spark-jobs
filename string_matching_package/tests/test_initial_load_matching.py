from typing import List

import pytest
from string_matching import initial_load_matching as victim


class TestPreprocessingOperators(object):

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo'),
                    ('2', 'NL', 'Dave Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '5312BE'),
                    ('3', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB'),
                    ('4', 'DE', 'Bruce Dickinson', 'Utrecht', 'Accacia Avenue', '22', '6666XX'), ]

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('id', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                     'zipCode')

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark)
        res = victim.preprocess_operators(ddf).collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = victim.preprocess_operators(ddf).select('matching_string').collect()
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
        return spark.createDataFrame(self.data).toDF('id', 'countryCode', 'firstName', 'lastName',
                                                     'city', 'street', 'houseNumber',
                                                     'zipCode', 'mobilePhoneNumber', 'emailAddress')

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark)
        res = victim.preprocess_contacts(ddf).collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = victim.preprocess_contacts(ddf).collect()
        assert res[0][2] == 'dave mustaine'
        assert res[0][6] == 'baravenue14b'


def gen_tuple(tup, n):
    return [(str(i),) + tup[1:-1] + (tup[-1].format(idx=i),) for i in range(n)]


class TestMatchingOperators(object):
    @classmethod
    def setup_class(cls):
        cls.data = gen_tuple(
            ('generated_id', 'NL', 'Dave Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b',
             'somemorecharacters{idx}BE'), 5)
        cls.data.extend([
            ('another_id', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB'),
        ])

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('id', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                     'zipCode')

    def test_full_matching(self, spark):
        ddf = self.create_ddf(spark)
        res = victim.apply_matching_on(ddf, spark,
                                       victim.preprocess_operators,
                                       victim.match_operators_for_country,
                                       'NL', 1500, 0.8).select('sourceId', 'targetId').sort('targetId').collect()
        assert len(res) == 4
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['0', '0', '0', '0']
        assert targets == ['1', '2', '3', '4']


class TestMatchingContactPersons(object):
    @classmethod
    def setup_class(cls):
        cls.data = gen_tuple(
            ('generated_id', 'NL', 'Dave', 'Mustaine ', 'Amsterdam  ', '@barAvenue', None, None, '5314BE',
             '\u09F214{idx}b'),
            4)

        cls.data.extend([
            ('1000', None, None, 'foo', 'foo', 'foo', 'phone', 'email', 'foo', 'foo'),
            ('1003', 'NL', 'Ritchie', 'Blackmore', 'Utrecht', 'fooStreet', '1234AB', None, None, '8'),
            ('1004', 'DE', 'Bruce', 'Dickinson', 'Utrecht', 'Accacia Avenue', '6666XX', None, None, '22'),
            ('1005', 'DE', None, None, 'bar', 'bar', '1234AB', None, None, '43'),
            ('1006', 'DE', '', '', 'bar', 'bar', '1234AB', None, None, '43'),
            ('1007', 'DE', 'firstName', 'emptyStreet', 'bar', '', '1234AB', None, None, None),
        ])

    def create_ddf(self, spark, extra_data: List[tuple]):
        return (spark.createDataFrame(self.data + extra_data)
                .toDF('id', 'countryCode', 'firstName', 'lastName',
                      'city', 'street', 'mobilePhoneNumber', 'emailAddress',
                      'zipCode', 'houseNumber'))

    @pytest.mark.skipif(True, reason="No way to access data inside an IDF object")
    def test_full_matching(self, spark):
        ddf = self.create_ddf(spark, [
            ('4', 'NL', 'Dave', 'Mustaire', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '\u09F2144b')])
        res = victim.apply_matching_on(ddf, spark,
                                       victim.preprocess_contacts,
                                       victim.match_contacts_for_country,
                                       'NL', 1500, 0.8).select('sourceId', 'targetId').sort('targetId').collect()
        assert len(res) == 5
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['0', '0', '0', '0']
        assert targets == ['1', '2', '3', '4']

    def test_full_matching_with_only_the_same_data(self, spark):
        ddf = self.create_ddf(spark, [])
        res = victim.apply_matching_on(ddf, spark,
                                       victim.preprocess_contacts,
                                       victim.match_contacts_for_country,
                                       'NL', 1500, 0.8).select('sourceId', 'targetId').sort('targetId').collect()
        assert len(res) == 3
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['0', '0', '0']
        assert targets == ['1', '2', '3']
