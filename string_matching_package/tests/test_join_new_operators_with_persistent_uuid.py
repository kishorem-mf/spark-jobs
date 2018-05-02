import join_new_operators_with_persistent_uuid as victim


class TestPreprocessing(object):

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo'),
                    ('2', 'NL', 'Dave Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '5312BE'),
                    ('3', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB'),
                    ('4', 'DE', 'Bruce Dickinson', 'Utrecht', 'Accacia Avenue', '22', '6666XX'), ]

    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                'zipCode')

    def test_should_not_drop_null_names(self, spark):
        ddf = self.create_ddf(spark, self.data)
        res = victim.preprocess_for_matching(ddf, 'concatId').collect()

        assert len(res) == 4
        assert ['1', '2', '3', '4'] == list(sorted([_[2] for _ in res]))

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark, self.data)
        res = victim.preprocess_for_matching(ddf, 'concatId', True).collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == list(sorted([_[2] for _ in res]))

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark, self.data)

        res = victim.preprocess_for_matching(ddf, 'concatId', True).select('matching_string').collect()
        assert res[0][0] == 'dave mustaine amsterdam baravenue14b 5312be'
