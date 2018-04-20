from pyspark import sql
from pyspark.sql import functions as sf

from string_matching import utils as victim


class TestGetCountries(object):
    @classmethod
    def setup_class(cls):
        countries = [('NL', 1), ('DE', 2), ('BE', 3), ('US', 101)]
        cls.country_data = [(k,) for k, v in countries for _ in range(v)]

    def test_should_by_default_select_over_100(self, spark):
        ddf = spark.createDataFrame(self.country_data).toDF("countryCode")
        selected_countries = victim.get_country_codes('all', ddf)
        assert len(selected_countries) == 1
        assert selected_countries[0] == 'US'

    def test_should_return_given_sizes(self, spark):
        ddf = spark.createDataFrame(self.country_data).toDF("countryCode")
        selected_countries = victim.get_country_codes('all', ddf, 2)
        assert len(selected_countries) == 2
        assert selected_countries[0] == 'BE'
        assert selected_countries[1] == 'US'

    def test_should_return_only_selected_country_if_large_enough(self, spark):
        ddf = spark.createDataFrame(self.country_data).toDF("countryCode")
        selected_countries = victim.get_country_codes('NL', ddf, 2)
        assert len(selected_countries) == 0

        selected_countries = victim.get_country_codes('BE', ddf, 2)
        assert len(selected_countries) == 1
        assert selected_countries[0] == 'BE'


class TestGroupMatches(object):

    @classmethod
    def setup_class(cls):
        cls.data = [(1, 2), (1, 3), (4, 3), ]

    def test_each_i_should_only_appear_once(self, spark):
        ddf = spark.createDataFrame(self.data).toDF('i', 'j')

        res = victim.group_matches(ddf).collect()

        assert len(res) == 2
        assert res[0] == (3, 1)
        assert res[1] == (2, 1)


class TestCleaning(object):

    @classmethod
    def setup_class(cls):
        cls.data = [('2', 'NL', 'Dave Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '5312BE')]

    def test_cleaning_should_remove_strange_chars(self, spark):
        ddf = spark.createDataFrame(self.data).toDF('id', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                    'zipCode')

        res = (victim
               .clean_operator_fields(ddf, 'name', 'city', 'street', 'houseNumber', 'zipCode')
               .select('nameCleansed', 'cityCleansed', 'streetCleansed', 'zipCodeCleansed')
               .collect()
               )

        assert res[0][0] == 'dave mustaine'
        assert res[0][1] == 'amsterdam'
        assert res[0][2] == 'baravenue\uFE3614b'
        assert res[0][3] == '5312be'


class TestMatchingString(object):

    @classmethod
    def setup_class(cls):
        cls.data = [('dave mustaine ', 'amsterdam  ', 'barAvenue\uFE3614b', '5312be')]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = spark.createDataFrame(self.data).toDF('nameCleansed', 'cityCleansed', 'streetCleansed', 'zipCodeCleansed')

        res = victim.create_operator_matching_string(ddf).select('matching_string').collect()
        assert res[0][0] == 'dave mustaine amsterdam baravenue14b 5312be'
