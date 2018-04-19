from pyspark import sql
from pyspark.sql import functions as sf

from string_matching import utils


class TestGetCountries(object):
    @classmethod
    def setup_class(cls):
        countries = [('NL', 1), ('DE', 2), ('BE', 3), ('US', 101)]
        cls.country_data = [(k,) for k, v in countries for _ in range(v)]

    def test_should_by_default_select_over_100(self, spark):
        ddf = spark.createDataFrame(self.country_data).toDF("countryCode")
        selected_countries = utils.get_country_codes('all', ddf)
        assert len(selected_countries) == 1
        assert selected_countries[0] == 'US'

    def test_should_return_given_sizes(self, spark):
        ddf = spark.createDataFrame(self.country_data).toDF("countryCode")
        selected_countries = utils.get_country_codes('all', ddf, 2)
        assert len(selected_countries) == 2
        assert selected_countries[0] == 'BE'
        assert selected_countries[1] == 'US'

    def test_should_return_only_selected_country_if_large_enough(self, spark):
        ddf = spark.createDataFrame(self.country_data).toDF("countryCode")
        selected_countries = utils.get_country_codes('NL', ddf, 2)
        assert len(selected_countries) == 0

        selected_countries = utils.get_country_codes('BE', ddf, 2)
        assert len(selected_countries) == 1
        assert selected_countries[0] == 'BE'


class TestGroupMatches(object):

    @classmethod
    def setup_class(cls):
        cls.data = [(1, 2), (1, 3), (4, 3), ]

    def test_each_i_should_only_appear_once(self, spark):
        ddf = spark.createDataFrame(self.data).toDF('i', 'j')

        res = utils.group_matches(ddf).collect()

        assert res[0] == (3, 1)
        assert res[1] == (2, 1)
